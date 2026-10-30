"""
Deep-learning experiments (PyTorch), same leave-one-season-out protocol and metrics as train_ml.py.

  gru      a GRU over the last W days of each county's features (shared across counties)
  stgnn    spatio-temporal GNN: each day, counties exchange information over a WIND-AWARE graph
           (upwind counties are sources) plus a static distance graph, then a GRU over W days
           (in the spirit of PM2.5-GNN, Wang et al. 2020, adapted to daily county data)

Usage:
    pip3 install torch
    python3 research/train_dl.py --smoke-test      # 2 epochs, checks everything runs (a few minutes on a laptop)
    python3 research/train_dl.py                   # full run; writes research_out/results/dl_*.csv
Device: CUDA or Apple MPS if available, else CPU.
"""
import argparse, json, os, sys, time
import numpy as np
import pandas as pd
import torch
import torch.nn as nn

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import config as C  # noqa: E402
import dl_data as DD  # noqa: E402
import evaluate as E  # noqa: E402

W = 7


class GRUModel(nn.Module):
    def __init__(self, f, h=64):
        super().__init__()
        self.inp = nn.Sequential(nn.Linear(f, h), nn.ReLU())
        self.gru = nn.GRU(h, h, batch_first=True)
        self.out = nn.Sequential(nn.Linear(h, h), nn.ReLU(), nn.Linear(h, 1))

    def forward(self, x, a=None, a0=None):            # x: B x W x N x F
        B, Wn, N, F = x.shape
        z = self.inp(x).permute(0, 2, 1, 3).reshape(B * N, Wn, -1)
        _, hN = self.gru(z)
        return self.out(hN[-1]).view(B, N)


class GraphLayer(nn.Module):
    """h_i' = ReLU(W_s h_i + W_w sum_j A_ij(t) h_j + W_d sum_j A0_ij h_j)."""
    def __init__(self, h):
        super().__init__()
        self.ws, self.ww, self.wd = nn.Linear(h, h), nn.Linear(h, h, bias=False), nn.Linear(h, h, bias=False)
        self.norm = nn.LayerNorm(h)

    def forward(self, h, a, a0):                      # h: B x N x H, a: B x N x N, a0: N x N
        return self.norm(torch.relu(self.ws(h) + self.ww(torch.bmm(a, h)) + self.wd(a0 @ h)) + h)


class STGNN(nn.Module):
    def __init__(self, f, h=64, layers=2):
        super().__init__()
        self.inp = nn.Sequential(nn.Linear(f, h), nn.ReLU())
        self.graph = nn.ModuleList([GraphLayer(h) for _ in range(layers)])
        self.gru = nn.GRU(h, h, batch_first=True)
        self.out = nn.Sequential(nn.Linear(h, h), nn.ReLU(), nn.Linear(h, 1))

    def forward(self, x, a, a0):                      # x: B x W x N x F, a: B x W x N x N
        B, Wn, N, F = x.shape
        z = self.inp(x)
        steps = []
        for k in range(Wn):
            h = z[:, k]
            for g in self.graph:
                h = g(h, a[:, k], a0)
            steps.append(h)
        z = torch.stack(steps, 1).permute(0, 2, 1, 3).reshape(B * N, Wn, -1)
        _, hN = self.gru(z)
        return self.out(hN[-1]).view(B, N)


def device():
    if torch.cuda.is_available():
        return torch.device("cuda")
    if getattr(torch.backends, "mps", None) and torch.backends.mps.is_available():
        return torch.device("mps")
    return torch.device("cpu")


def batches(idx, bs, rng=None):
    idx = np.array(idx)
    if rng is not None:
        rng.shuffle(idx)
    for i in range(0, len(idx), bs):
        yield idx[i:i + bs]


def tensors(Xs, A, Y, M, ts, dev):
    win = np.stack([np.arange(t - W + 1, t + 1) for t in ts])
    return (torch.from_numpy(Xs[win]).to(dev), torch.from_numpy(A[win]).to(dev),
            torch.from_numpy(Y[ts]).to(dev), torch.from_numpy(M[ts]).to(dev))


def train_fold(name, P, Xs, A, A0, test_season, epochs, dev, seed=0):
    torch.manual_seed(seed); rng = np.random.default_rng(seed)
    T = len(P["dates"]); S = P["season"]
    same_season = np.array([t >= W - 1 and S[t - W + 1] == S[t] for t in range(T)])
    train_t = [t for t in range(T) if same_season[t] and S[t] != test_season]
    test_t = [t for t in range(T) if same_season[t] and S[t] == test_season]
    val_t = list(rng.choice(train_t, max(1, len(train_t) // 7), replace=False)); vs = set(val_t)
    fit_t = [t for t in train_t if t not in vs]
    model = (GRUModel if name == "gru" else STGNN)(Xs.shape[-1]).to(dev)
    opt = torch.optim.AdamW(model.parameters(), lr=1e-3, weight_decay=1e-4)
    a0 = torch.from_numpy(A0).to(dev)
    best, best_state, bad = np.inf, None, 0
    for ep in range(epochs):
        model.train()
        for b in batches(fit_t, 16, rng):
            x, a, y, m = tensors(Xs, A, P["Y"], P["M"], b, dev)
            loss = (((model(x, a, a0) - y) ** 2) * m).sum() / m.sum().clamp(min=1)
            opt.zero_grad(); loss.backward(); nn.utils.clip_grad_norm_(model.parameters(), 1.0); opt.step()
        model.eval(); se = n = 0.0
        with torch.no_grad():
            for b in batches(val_t, 32):
                x, a, y, m = tensors(Xs, A, P["Y"], P["M"], b, dev)
                se += float((((model(x, a, a0) - y) ** 2) * m).sum()); n += float(m.sum())
        v = se / max(n, 1)
        if v < best - 1e-4:
            best, best_state, bad = v, {k: t.detach().clone() for k, t in model.state_dict().items()}, 0
        else:
            bad += 1
        if bad >= 8:
            break
    model.load_state_dict(best_state)
    model.eval(); rows = []
    with torch.no_grad():
        for b in batches(test_t, 32):
            x, a, y, m = tensors(Xs, A, P["Y"], P["M"], b, dev)
            p = model(x, a, a0).cpu().numpy()
            for k, t in enumerate(b):
                for j in np.where(P["M"][t] > 0)[0]:
                    rows.append((P["counties"][j], P["dates"][t], float(np.expm1(np.clip(p[k, j], 0, np.log1p(1000.0))))))
    print(f"  {name:6s} test season {test_season}: best val MSE(log) {best:.4f}, {ep + 1} epochs")
    return pd.DataFrame(rows, columns=["county_fips", "date", f"pred_{name}"])


def main(smoke_test=False):
    t0 = time.time(); dev = device(); print(f"device: {dev}")
    df = E.prepare_features(pd.read_csv(os.path.join(C.OUT, "features.csv.gz"), dtype={"county_fips": str}))
    df["date"] = pd.to_datetime(df.date).dt.date
    g = json.load(open(os.path.join(C.OUT, "feature_groups.json")))
    feats = g["base"] + g["fire"] + g["transport"] + g["hms"]
    seasons = sorted(df.season.unique())
    P = DD.build_panel(df, feats, seasons)
    lat, lon = DD.county_coords(df, P["counties"])
    A, A0 = DD.wind_graph(lat, lon, P["WF"])
    epochs = 2 if smoke_test else 80
    preds = []
    for s in seasons:
        Xs = DD.standardise(P["X"], P["season"] != s)        # scaling fitted on training seasons only
        fold = None
        for name in ["gru", "stgnn"]:
            p = train_fold(name, P, Xs, A, A0, s, epochs, dev)
            fold = p if fold is None else fold.merge(p, on=["county_fips", "date"])
        preds.append(fold)
    preds = pd.concat(preds).merge(df[["county_fips", "date", "season", "y_pm25_next", "smoke_context", "pm25_t", "pm25_neigh_t"]],
                                   on=["county_fips", "date"])
    preds["pred_persistence"] = preds.pm25_t.fillna(preds.pm25_neigh_t).fillna(preds.y_pm25_next.median())
    res = os.path.join(C.OUT, "results"); os.makedirs(res, exist_ok=True)
    preds.to_csv(os.path.join(res, "dl_predictions_leave_season_out.csv.gz"), index=False)
    rows = []
    for col in ["pred_gru", "pred_stgnn", "pred_persistence"]:
        for subset, sub in [("all", preds), ("smoke_context", preds[preds.smoke_context == 1])]:
            rows.append({"model": col[5:], "subset": subset, **E.point_metrics(sub.y_pm25_next, sub[col])})
        if col != "pred_persistence":
            dm, pv = E.diebold_mariano(preds, col, "pred_persistence")
            lo, hi = E.bootstrap_ci(preds, col, "rmse", n=100 if smoke_test else 1000)
            rows[-2].update({"dm_vs_persistence": dm, "dm_p_value": pv, "rmse_ci_low": lo, "rmse_ci_high": hi})
    out = pd.DataFrame(rows); out.to_csv(os.path.join(res, "dl_comparison.csv"), index=False)
    print(out.round(3).to_string(index=False)); print(f"done in {time.time() - t0:.0f}s")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--smoke-test", action="store_true")
    main(ap.parse_args().smoke_test)
