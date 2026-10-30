# Upwind or Nearby? Forecasting Next-Day Wildfire-Smoke PM2.5 in California from Open Satellite Fire Data with a Wind-Aware Transport Index

*Draft. Methods are written; every `[R: …]` is a result to fill from `research_out/` after the real-data runs. Do not fill from synthetic tests.*

Authors: Group 8 (Nikhil Sai Viswajith, Nino Pelko, Nithish Balasubramanian, Ragavedhni Kuppa Ramesh), San José State University

## Abstract (≈200 words, finalise last)
Wildfire smoke now drives the worst fine-particulate (PM2.5) days in the western United States, but operational smoke forecasts
depend on chemical transport models that are costly to run. We ask how far freely available data alone can go. We build a
reproducible lakehouse pipeline that integrates NASA FIRMS VIIRS fire detections, EPA AirData PM2.5, NOAA GHCN-Daily weather and
NOAA HMS smoke analyses for four California fire seasons (2018–2021), and forecast next-day county-mean PM2.5 and exceedances of the
35 µg/m³ standard. Our central feature is a wind-aware transport index that sums fire radiative power from fires lying upwind of
each county, discounted by distance. To test whether its value comes from transport rather than from fire proximity, we compare it
with the same index computed downwind (a placebo). Under leave-one-season-out evaluation, [R: best model] reduces RMSE on
smoke-affected days by [R: x%] relative to persistence (Diebold–Mariano p = [R]); the upwind index outperforms its downwind
placebo by [R]; and a wind-graph spatio-temporal GNN [R: does / does not] improve on gradient boosting. [R: one sentence on
leave-one-region-out]. Code and data pipeline are open.

## 1 Introduction
- Smoke-driven PM2.5 episodes are increasing in California; next-day warnings support public-health action.
- Process models (e.g. smoke forecasting systems) are expensive; ML on open data is a cheap complement.
- Prior ML work uses rich inputs (chemical-transport model output, satellite AOD, reanalysis): Reid et al. (2015) predicted PM2.5 during
  the 2008 northern California fires with 29 predictors incl. CTM output and AOD; Raffuse et al. (2024) estimate daily exposure with
  random forests over monitors, low-cost sensors, AOD and a smoke forecast; Liao et al. (arXiv 2312.04291) forecast hourly PM2.5 at
  California monitors with a spatio-temporal GNN using VIIRS FRP with inverse-distance and wind weighting and ERA5 reanalysis;
  Wang et al. (2020) introduced PM2.5-GNN, a wind-aware graph network for PM2.5 forecasting.
- Gap we target: (i) **open, routinely available data only** (no CTM, no reanalysis), at the county level used for public advisories;
  (ii) a **falsification test** of the transport mechanism (upwind vs downwind placebo) rather than only an accuracy gain;
  (iii) **out-of-season and out-of-region generalisation** with significance testing, stratified to smoke days where errors matter.
- Contributions: (1) reproducible Bronze/Silver/Gold pipeline and open feature set; (2) wind-aware transport index + placebo design;
  (3) a benchmark of persistence, linear, tree ensembles, MLP, GRU and a wind-graph ST-GNN under identical protocols.

## 2 Data
| Source | Variable | Resolution | Use |
| --- | --- | --- | --- |
| EPA AirData daily_88101 | PM2.5 (FRM/FEM, 24-h mean) | site-day | target, persistence features |
| NASA FIRMS VIIRS S-NPP (SP) | fire location, FRP (MW) | 375 m, ~2 overpasses/day | fire and transport features |
| NOAA GHCN-Daily (first-order stations) | TMAX, TMIN, PRCP, AWND, WDF2 | station-day | weather, wind direction |
| NOAA HMS smoke polygons | analyst smoke density (light/medium/heavy) | daily polygons | feature + independent cross-check |
| Census 2020 gazetteer | county internal points | 58 counties | spatial key |

Study period: 1 Jul – 30 Nov, 2018–2021. Fires within (−126, 31, −112, 44.5) so out-of-state fires count. [R: rows, counties,
exceedance share per season — from build_dataset output.]

## 3 Methods
**3.1 Pipeline.** Raw files land in Bronze (append-only Delta, file hash manifest); Silver applies validation (17 rules), quarantine and
de-duplication (AirData repeats each site-day per pollutant standard; wildfire "Excluded" rows are dropped in favour of "Included");
the county-day Gold table feeds modelling. FIRMS acquisition times (UTC) are converted to the local date.

**3.2 Target.** y = county-mean PM2.5 on day t+1 (mean over sites, each site averaged over POCs); exceedance = y > 35 µg/m³.
Features use data up to day t only. HMS for day t is published by the next morning, so it is available at forecast time for t+1.

**3.3 Features.** *Base:* PM2.5 at t, t−1, t−2, 3-day mean, inverse-distance mean of neighbouring counties within 300 km; weather
from the three nearest first-order stations; wind as a unit vector of the wind-from direction; day-of-year harmonics; latitude,
longitude. *Fire:* detection counts and summed FRP within 50/100/200/400 km at t and t−1; distance to nearest fire.
*Transport (ours):* for county c with wind-from direction φ_c and fires f at distance d_cf and bearing β_cf,

```latex
T_c^{(L)} = \sum_{f:\, d_{cf} \le 600\,\mathrm{km}} \mathrm{FRP}_f \cdot \max\!\big(0, \cos(\beta_{cf} - \varphi_c)\big) \cdot e^{-d_{cf}/L}, \quad L \in \{100, 300\}\ \mathrm{km}
```

*Placebo:* the same sum with max(0, −cos(β − φ)), i.e. fires lying downwind. *HMS:* highest smoke density over the county point.
Heavy-tailed counts and FRP sums are log1p-transformed.

**3.4 Models.** Persistence (PM2.5 at t, falling back to neighbours); monthly county climatology; ridge; random forest; histogram
gradient boosting; MLP — all regression models fit on log1p(PM2.5). Deep models: a GRU over 7-day windows per county, and a
spatio-temporal GNN in which counties exchange hidden states each day over a wind-aware graph
(A_ij(t) ∝ max(0, cos(bearing(i→j) − φ_i(t))) · e^{−d_ij/300 km}, row-normalised) and a static distance graph, followed by a GRU.

**3.5 Evaluation.** (a) Leave-one-season-out (train on three seasons, test on the fourth); (b) leave-one-region-out (k-means
clusters of county locations, k = 5). Metrics: RMSE, MAE, R²; exceedance precision, recall, F1, AUPRC. Reported for all days and for
*smoke-context* days (PM2.5 > 35 on t or t+1, or HMS medium/heavy on t). 95% CIs by day-block bootstrap (1,000 resamples); paired
comparisons by Diebold–Mariano tests on daily mean squared-error differentials (Newey–West, lag 1). Ablations add feature groups to
a gradient-boosting model; the key contrast is A2 (upwind) vs A3 (downwind placebo).

## 4 Results
**4.1 Overall skill.** Table 2: [R: model_comparison.csv + dl_comparison.csv, both protocols, all / smoke-context].
**4.2 Does transport matter?** Table 3 / Fig. 3: [R: ablation.csv; A2 vs A1, A2 vs A3 with DM p-values].
**4.3 What drives the forecasts?** Fig. 4: [R: permutation importance].
**4.4 Case study.** Fig. 5: [R: worst county, held-out season; where persistence lags the onset and the model anticipates it, or not].
**4.5 Cross-check with HMS.** Table 4: [R: hms_crosscheck.csv — exceedance rate by HMS density class].

## 5 Discussion
- Interpret A2 vs A3: an upwind advantage supports a transport mechanism; no advantage means the index acts as a fire-proximity proxy.
- Where DL helps or does not (data size: ~58 counties × ~600 days is small for deep models).
- Limitations: county internal point instead of population-weighted exposure; surface wind at stations, not transport-level winds;
  sparse monitors (counties without monitors are not evaluated); HMS is a qualitative analyst product; daily resolution hides
  diurnal plume dynamics; four seasons limit leave-one-season-out to four folds.

## 6 Conclusion
[R: two sentences.]

## Data and code availability
https://github.com/Nithish09/wildfire-smoke-pipeline (pipeline in `notebooks/`, study in `research/`).

## References
- Reid, C. E., Jerrett, M., Petersen, M. L., Pfister, G. G., Morefield, P. E., Tager, I. B., Raffuse, S. M., & Balmes, J. R. (2015).
  Spatiotemporal prediction of fine particulate matter during the 2008 Northern California wildfires using machine learning.
  *Environmental Science & Technology*, 49(6), 3887–3896. https://doi.org/10.1021/es505846r
- Wang, S., Li, Y., Zhang, J., Meng, Q., Meng, L., & Gao, F. (2020). PM2.5-GNN: A domain knowledge enhanced graph neural network for
  PM2.5 forecasting. https://doi.org/10.1145/3397536.3422208 (arXiv:2002.12898)
- Liao, K., Buch, J., Lamb, K., & Gentine, P. Simulating the air quality impact of prescribed fires using graph neural network-based
  PM2.5 forecasts. arXiv:2312.04291
- Raffuse, S., O'Neill, S., & Schmidt, R. (2024). A model for rapid PM2.5 exposure estimates in wildfire conditions using routinely
  available data: rapidfire v0.1.3. *Geoscientific Model Development*, 17, 381–397.
- Wang, R., Bhudia, A., Dos Remedios, B., Teng, M., & Ng, R. Dense forecasting of wildfire smoke particulate matter using sparsity
  invariant convolutional neural networks. arXiv:2009.11362
- Data: NASA FIRMS; US EPA AirData; NOAA NCEI GHCN-Daily; NOAA OSPO Hazard Mapping System; US Census Bureau 2020 Gazetteer.

*AI-use statement (required by most venues): code and this draft were developed with an AI assistant (Claude) and reviewed by the authors.*
