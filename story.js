const COLORS = {
  selected: "#0F766E",
  unselected: "#F59E0B",
  none: "#E5E7EB"
};

// ?data=latest shows the most recent Airflow run (data/watchlist_latest/);
// the default is the saved snapshot the written story describes.
const DATA_SETS = {
  snapshot: "data/watchlist",
  latest: "data/watchlist_latest"
};
const dataKey = new URLSearchParams(location.search).get("data");
const DATA_DIR = DATA_SETS[dataKey] ?? DATA_SETS.snapshot;

async function loadStory() {
  const [summary, metrics, workload,    totals] =
    await Promise.all([
      d3.csv(`${DATA_DIR}/stock_summary.csv`),
      d3.csv(`${DATA_DIR}/stock_metrics.csv`),
      d3.csv(`${DATA_DIR}/daily_workload.csv`),
      d3.csv(`${DATA_DIR}/workload_summary.csv`)
    ]);

  // Verify that the full dataset is present.
  const dates = [...new Set(metrics.map(d => d.date))].sort();
  const tickers = summary.map(d => d.ticker);

  // Every ticker must have one row per trading date.
  if (dates.length === 0 || metrics.length !== tickers.length * dates.length) {
    throw new Error("Expected one row per ticker per trading date.");
  }

  if (DATA_DIR !== DATA_SETS.snapshot) {
    const banner = document.getElementById("data-banner");
    banner.hidden = false;
    banner.textContent =
      `Showing the latest pipeline run (${dates[0]} to ` +
      `${dates[dates.length - 1]}). The table and grid use this data; ` +
      "the written story below describes the saved Aug 21 to Oct 2, 2026 snapshot.";
  }

  // ----- View 1: Summary table -----

  const tbody = d3.select("#results-table");

  // Keep the story's NVDA, TSLA, SPY order; any other tickers follow.
  const preferred = ["NVDA", "TSLA", "SPY"];
  const rank = t => (preferred.includes(t) ? preferred.indexOf(t) : 99);
  summary.sort((a, b) => rank(a.ticker) - rank(b.ticker));
  tickers.sort((a, b) => rank(a) - rank(b));

  const rows = tbody.selectAll("tr")
    .data(summary)
    .join("tr");

  rows.each(function(d) {
    const values = [
      d.ticker,
      `${Number(d.window_return_pct).toFixed(2)}%`,
      `${Number(d.avg_daily_swing_pct).toFixed(2)}%`,
      d.alerts_3pct,
      d.alerts_5pct
    ];

    d3.select(this)
      .selectAll("td")
      .data(values)
      .join("td")
      .text(value => value);
  });

  // ----- View 2: Interactive alert grid -----

  function drawChart(threshold) {
    const selectedThreshold = Number(threshold);

    const currentWorkload = workload.filter(
      d => Number(d.threshold_pct) === selectedThreshold
    );

    const workloadByDate = new Map(
      currentWorkload.map(d => [d.date, d])
    );

    const total = totals.find(
      d => Number(d.threshold_pct) === selectedThreshold
    );

    if (!total || currentWorkload.length !== dates.length) {
      throw new Error("Missing workload results for this threshold.");
    }

    d3.select("#workload-description").text(
      `At ${selectedThreshold}%, there are ` +
      `${total.total_asset_alerts} asset alerts across ` +
      `${total.dates_with_alerts} distinct dates. ` +
      `${total.simultaneous_alert_dates} dates have competing ` +
      `alerts, leaving ${total.unreviewed_alerts} alerts without ` +
      `a separate manual review.`
    );

    const width = 1050;
    const height = 260;

    const svg = d3.select("#alert-chart")
      .attr("viewBox", `0 0 ${width} ${height}`);

    // Clear the previous chart so controls do not duplicate bars.
    svg.selectAll("*").remove();

    const plot = svg.append("g")
      .attr("transform", "translate(80,35)");

    const x = d3.scaleBand()
      .domain(dates)
      .range([0, 930])
      .padding(0.12);

    const y = d3.scaleBand()
      .domain(tickers)
      .range([0, 150])
      .padding(0.18);

    plot.selectAll("rect")
      .data(metrics)
      .join("rect")
      .attr("x", d => x(d.date))
      .attr("y", d => y(d.ticker))
      .attr("width", x.bandwidth())
      .attr("height", y.bandwidth())
      .attr("rx", 2)
      .attr("fill", d => {
        const alert =
          Number(d.daily_swing_pct) >= selectedThreshold;

        const chosen =
          workloadByDate.get(d.date)?.selected_asset === d.ticker;

        if (!alert) return COLORS.none;
        return chosen ? COLORS.selected : COLORS.unselected;
      })
      .append("title")
      .text(d => {
        const alert =
          Number(d.daily_swing_pct) >= selectedThreshold;

        const chosen =
          alert &&
          workloadByDate.get(d.date)?.selected_asset === d.ticker;

        return (
          `${d.ticker} | ${d.date}\n` +
          `Daily swing: ${Number(d.daily_swing_pct).toFixed(2)}%\n` +
          (chosen
            ? "Selected for review"
            : alert
              ? "Alert, not selected"
              : "Below threshold")
        );
      });

    // Ticker labels.
    plot.append("g")
      .call(d3.axisLeft(y).tickSize(0))
      .select(".domain").remove();

    // Date labels.
    const shownDates = dates.filter(
      (date, i) => i % 5 === 0 || i === dates.length - 1
    );

    plot.append("g")
      .attr("transform", "translate(0,150)")
      .call(
        d3.axisBottom(x)
          .tickValues(shownDates)
          .tickFormat(date => date.slice(5))
          .tickSize(4)
      )
      .selectAll("text")
      .attr("font-size", 12);

    plot.append("text")
      .attr("x", 465)
      .attr("y", 200)
      .attr("text-anchor", "middle")
      .attr("font-size", 14)
      .text(`Trading date (${dates[0].slice(0, 4)})`);
  }

  // Update when the threshold changes.
  d3.select("#threshold").on("change", function() {
    drawChart(this.value);
  });

  // Draw the default threshold immediately.
  drawChart(3);
}

loadStory().catch(error => {
  console.error(error);

  document.getElementById("error-message").textContent =
    `Could not load the data: ${error.message}`;
});