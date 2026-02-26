import React, { useEffect, useMemo, useState } from "react";

const API_BASE = import.meta.env.VITE_API_BASE || "http://localhost:8000";

function formatPercent(value) {
  if (value === null || value === undefined) return "-";
  const pct = Number(value) * 100;
  if (Number.isNaN(pct)) return "-";
  return `${pct.toFixed(1)}%`;
}

function formatNumber(value) {
  if (value === null || value === undefined) return "-";
  return new Intl.NumberFormat("ko-KR").format(Number(value));
}

function useFetchJson(url, deps = []) {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    let alive = true;
    setLoading(true);
    setError(null);

    fetch(url)
      .then(async (res) => {
        if (res.status === 204) return null;
        if (!res.ok) {
          const text = await res.text();
          throw new Error(text || `HTTP ${res.status}`);
        }
        return res.json();
      })
      .then((json) => {
        if (!alive) return;
        setData(json);
      })
      .catch((err) => {
        if (!alive) return;
        setError(err);
      })
      .finally(() => {
        if (!alive) return;
        setLoading(false);
      });

    return () => {
      alive = false;
    };
  }, deps);

  return { data, loading, error };
}

function MarkdownLite({ content }) {
  if (!content) return <div className="empty">내용이 없습니다.</div>;
  const lines = content.split("\n");
  return (
    <div className="report-body">
      {lines.map((line, idx) => {
        if (line.startsWith("# ")) {
          return (
            <h2 key={idx}>
              {line.replace(/^#\s+/, "")}
            </h2>
          );
        }
        if (line.startsWith("## ")) {
          return (
            <h3 key={idx}>
              {line.replace(/^##\s+/, "")}
            </h3>
          );
        }
        if (line.startsWith("- ")) {
          return (
            <div key={idx} className="report-bullet">
              <span className="bullet-dot" />
              <span>{line.replace(/^-\s+/, "")}</span>
            </div>
          );
        }
        if (!line.trim()) {
          return <div key={idx} className="report-space" />;
        }
        return (
          <p key={idx}>
            {line}
          </p>
        );
      })}
    </div>
  );
}

export default function App() {
  const [tab, setTab] = useState("dashboard");
  const [showAllTop, setShowAllTop] = useState(false);
  const [reportQuery, setReportQuery] = useState("");
  const [reportRange, setReportRange] = useState("all");

  const dashboardUrl = `${API_BASE}/api/dashboard/latest`;
  const reportsUrl = `${API_BASE}/api/reports?limit=30`;

  const dashboard = useFetchJson(dashboardUrl, [dashboardUrl]);
  const reports = useFetchJson(reportsUrl, [reportsUrl]);

  const [selectedDate, setSelectedDate] = useState(null);
  const reportDetailUrl = selectedDate ? `${API_BASE}/api/reports/${selectedDate}` : null;
  const reportDetail = useFetchJson(
    reportDetailUrl || "",
    [reportDetailUrl]
  );

  const traffic = dashboard.data?.traffic || null;
  const topItems = dashboard.data?.top?.items || [];
  const risingItems = dashboard.data?.rising?.items || [];

  const topItemsVisible = useMemo(() => {
    if (showAllTop) return topItems;
    return topItems.slice(0, 10);
  }, [topItems, showAllTop]);

  const reportItems = useMemo(() => {
    const items = reports.data?.items || [];
    const now = new Date();
    const query = reportQuery.trim().toLowerCase();
    return items.filter((item) => {
      if (reportRange !== "all") {
        const days = reportRange === "7" ? 7 : 30;
        const date = new Date(item.report_date);
        const diff = (now - date) / (1000 * 60 * 60 * 24);
        if (Number.isNaN(diff) || diff > days) return false;
      }
      if (!query) return true;
      const hay = `${item.report_date || ""} ${item.title || ""}`.toLowerCase();
      return hay.includes(query);
    });
  }, [reports.data, reportQuery, reportRange]);

  useEffect(() => {
    if (!selectedDate && reportItems.length) {
      setSelectedDate(reportItems[0].report_date);
    }
  }, [reportItems, selectedDate]);

  return (
    <div className="app">
      <div className="bg-orb orb-1" />
      <div className="bg-orb orb-2" />
      <header className="topbar">
        <div className="brand">
          <span className="brand-mark" />
          <div>
            <div className="brand-title">Trend Platform</div>
            <div className="brand-sub">Realtime & Daily Insight</div>
          </div>
        </div>
        <div className="topbar-actions">
          <button
            className={`tab-btn ${tab === "dashboard" ? "active" : ""}`}
            onClick={() => setTab("dashboard")}
          >
            실시간 대시보드
          </button>
          <button
            className={`tab-btn ${tab === "reports" ? "active" : ""}`}
            onClick={() => setTab("reports")}
          >
            일일 보고서
          </button>
        </div>
      </header>

      <main className="content">
        {tab === "dashboard" && (
          <section className="grid">
            <article className="card hero">
              <div className="card-head">
                <h2>최근 10분 트래픽</h2>
                <span className="pill">1분 슬라이드</span>
              </div>
              {dashboard.loading && <div className="empty">로딩 중...</div>}
              {dashboard.error && (
                <div className="empty">데이터를 불러오지 못했습니다.</div>
              )}
              {traffic && (
                <div className="traffic">
                  <div className="traffic-main">
                    <div>
                      <div className="label">최근 10분</div>
                      <div className="value">{formatNumber(traffic.total_posts)}</div>
                    </div>
                    <div>
                      <div className="label">직전 10분</div>
                      <div className="value muted">{formatNumber(traffic.prev_total_posts)}</div>
                    </div>
                    <div>
                      <div className="label">증감률</div>
                      <div className={`value ${traffic.traffic_increase_rate >= 0 ? "up" : "down"}`}>
                        {traffic.traffic_increase_rate === null
                          ? "-"
                          : `${traffic.traffic_increase_rate.toFixed(1)}%`}
                      </div>
                    </div>
                  </div>
                  <div className="meta">
                    집계 기준 시각: {traffic.window_end || "-"}
                  </div>
                </div>
              )}
            </article>

            <article className="card">
              <div className="card-head">
                <h2>실시간 Top 단어</h2>
                <span className="pill">Top 20</span>
              </div>
              {topItemsVisible.length === 0 && <div className="empty">데이터 없음</div>}
              {topItemsVisible.map((item) => (
                <div className="rank-row" key={item.rank || item.token}>
                  <div className="rank">{item.rank}</div>
                  <div className="token">
                    <div className="token-name">{item.token}</div>
                    <div className="token-meta">
                      {formatNumber(item.count)} · 점유율 {formatPercent(item.share)}
                    </div>
                  </div>
                  <div className="delta">
                    {item.is_new ? "NEW" : `${formatPercent(item.increase_rate)}`}
                  </div>
                  <div className="bar">
                    <span style={{ width: `${Math.min(100, (item.share || 0) * 1000)}%` }} />
                  </div>
                </div>
              ))}
              {topItems.length > 10 && (
                <button className="ghost" onClick={() => setShowAllTop((v) => !v)}>
                  {showAllTop ? "접기" : "더보기"}
                </button>
              )}
            </article>

            <article className="card">
              <div className="card-head">
                <h2>급상승 단어</h2>
                <span className="pill">Rising</span>
              </div>
              {risingItems.length === 0 && <div className="empty">데이터 없음</div>}
              {risingItems.slice(0, 10).map((item) => (
                <div className="rank-row" key={item.token}>
                  <div className="rank">{item.rank || "-"}</div>
                  <div className="token">
                    <div className="token-name">{item.token}</div>
                    <div className="token-meta">
                      {formatNumber(item.prev_count)} → {formatNumber(item.count)}
                    </div>
                  </div>
                  <div className="delta">
                    {item.is_new ? "NEW" : `${formatPercent(item.increase_rate)}`}
                  </div>
                  <div className="bar">
                    <span style={{ width: `${Math.min(100, (item.increase_rate || 0) * 40)}%` }} />
                  </div>
                </div>
              ))}
            </article>
          </section>
        )}

        {tab === "reports" && (
          <section className="reports">
            <aside className="report-list">
              <div className="card-head">
                <h2>보고서 리스트</h2>
                <span className="pill">Daily</span>
              </div>
              <div className="report-filters">
                <input
                  className="filter-input"
                  placeholder="날짜/제목 검색"
                  value={reportQuery}
                  onChange={(e) => setReportQuery(e.target.value)}
                />
                <div className="filter-row">
                  <button
                    className={`chip ${reportRange === "all" ? "active" : ""}`}
                    onClick={() => setReportRange("all")}
                  >
                    전체
                  </button>
                  <button
                    className={`chip ${reportRange === "7" ? "active" : ""}`}
                    onClick={() => setReportRange("7")}
                  >
                    7일
                  </button>
                  <button
                    className={`chip ${reportRange === "30" ? "active" : ""}`}
                    onClick={() => setReportRange("30")}
                  >
                    30일
                  </button>
                </div>
              </div>
              {reports.loading && <div className="empty">로딩 중...</div>}
              {reports.error && <div className="empty">목록을 불러오지 못했습니다.</div>}
              {reportItems.map((item) => (
                <button
                  key={item.report_date}
                  className={`report-item ${selectedDate === item.report_date ? "active" : ""}`}
                  onClick={() => setSelectedDate(item.report_date)}
                >
                  <div className="report-date">{item.report_date}</div>
                  <div className="report-title">{item.title || "리포트"}</div>
                </button>
              ))}
            </aside>

            <article className="card report-detail">
              <div className="card-head">
                <h2>보고서 상세</h2>
                <div className="report-actions">
                  <span className="pill">Report</span>
                  <button className="ghost" onClick={() => window.print()}>
                    PDF 다운로드
                  </button>
                </div>
              </div>
              {reportDetail.loading && <div className="empty">로딩 중...</div>}
              {reportDetail.error && <div className="empty">보고서를 불러오지 못했습니다.</div>}
              {!reportDetail.loading && reportDetail.data && (
                <div>
                  <div className="report-header">
                    <div className="report-date">{reportDetail.data.report_date}</div>
                    <div className="report-title">
                      {reportDetail.data.title || "Daily Trend Report"}
                    </div>
                    {reportDetail.data.summary && (
                      <div className="report-summary">{reportDetail.data.summary}</div>
                    )}
                    {Array.isArray(reportDetail.data.keywords) && reportDetail.data.keywords.length > 0 && (
                      <div className="tag-list">
                        {reportDetail.data.keywords.map((kw, idx) => (
                          <span className="tag" key={`${kw}-${idx}`}>{kw}</span>
                        ))}
                      </div>
                    )}
                  </div>
                  <MarkdownLite content={reportDetail.data.content_md} />
                </div>
              )}
            </article>
          </section>
        )}
      </main>
    </div>
  );
}
