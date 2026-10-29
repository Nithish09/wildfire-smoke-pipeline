# Final presentation outline (10–12 slides, ~12 minutes)

1. **Title** – project name, Group 8, members.
2. **Problem** – smoke travels; fire, air-quality and weather data live in separate systems with different formats and grains.
3. **Research questions** – RQ1 integration, RQ2 CSV vs API, RQ3 incremental reruns and late data.
4. **Data sources** – FIRMS, EPA AQS, NOAA; area, window (15 Aug – 30 Sep 2020), volumes [FILL].
5. **Architecture** – `docs/architecture.png`; one line per layer.
6. **Silver: quality rules** – quarantine reasons chart; pass rate per source [FILL]; the EPA duplicate story.
7. **Gold: star schema** – fact + dimensions; 58 counties × days.
8. **RQ3 demo** – rerun → 0 changes (`DESCRIBE HISTORY` metrics); late correction → 1 county-day changed.
9. **RQ2 + benchmark** – CSV vs JSON timings; heaviest pipeline step [FILL].
10. **Dashboard** – statewide fire FRP vs PM2.5; worst counties; smoke-indicator mix.
11. **Limitations and lessons** – nearest-centroid lookup, sparse monitors, descriptive indicator, free-tier quotas.
12. **Summary + questions.**

Speaker split suggestion: Nikhil 2–4, Ragavedhni 5 and 11, Nino 6–8, Nithish 9–10, everyone 12.
