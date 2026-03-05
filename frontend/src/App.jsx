import React, { useEffect, useMemo, useState } from "react";
import DashboardView from "@/components/DashboardView";
import ReportsView from "@/components/ReportsView";
import Topbar from "@/components/Topbar";

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

export default function App() {
  const [tab, setTab] = useState(() => {
    if (typeof window === "undefined") return "dashboard";
    return window.location.pathname.startsWith("/reports") ? "reports" : "dashboard";
  });
  const [showAllTop, setShowAllTop] = useState(false);
  const [reportQuery] = useState("");
  const [reportRange, setReportRange] = useState("all");
  const [reportDate, setReportDate] = useState("");
  const [reportsRefreshTick, setReportsRefreshTick] = useState(0);

  const dashboardUrl = `${API_BASE}/api/dashboard/latest`;
  const reportsUrl = `${API_BASE}/api/reports?limit=30`;

  const dashboard = useFetchJson(dashboardUrl, [dashboardUrl]);
  const reports = useFetchJson(reportsUrl, [reportsUrl, reportsRefreshTick]);

  const [selectedReport, setSelectedReport] = useState(null);
  const reportDetailUrl = selectedReport
    ? `${API_BASE}/api/reports/${selectedReport.report_date}?version=${selectedReport.version}`
    : null;
  const reportDetail = useFetchJson(reportDetailUrl || "", [
    reportDetailUrl,
    reportsRefreshTick,
  ]);

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
      if (reportDate) {
        if (item.report_date !== reportDate) return false;
      } else if (reportRange !== "all") {
        const days = reportRange === "7" ? 7 : 30;
        const date = new Date(item.report_date);
        const diff = (now - date) / (1000 * 60 * 60 * 24);
        if (Number.isNaN(diff) || diff > days) return false;
      }
      if (!query) return true;
      const hay = `${item.report_date || ""} ${item.title || ""}`.toLowerCase();
      return hay.includes(query);
    });
  }, [reports.data, reportDate, reportQuery, reportRange]);

  useEffect(() => {
    if (!reportItems.length) return;
    if (!selectedReport) {
      setSelectedReport({
        report_date: reportItems[0].report_date,
        version: reportItems[0].version,
      });
      return;
    }
    const exists = reportItems.some(
      (item) =>
        item.report_date === selectedReport.report_date &&
        item.version === selectedReport.version
    );
    if (!exists) {
      setSelectedReport({
        report_date: reportItems[0].report_date,
        version: reportItems[0].version,
      });
    }
  }, [reportItems, selectedReport]);

  useEffect(() => {
    const onPopState = () => {
      const nextTab = window.location.pathname.startsWith("/reports")
        ? "reports"
        : "dashboard";
      setTab(nextTab);
    };
    window.addEventListener("popstate", onPopState);
    return () => window.removeEventListener("popstate", onPopState);
  }, []);

  useEffect(() => {
    if (typeof window === "undefined") return;
    const targetPath = tab === "reports" ? "/reports" : "/";
    if (window.location.pathname !== targetPath) {
      const nextUrl = `${targetPath}${window.location.search}${window.location.hash}`;
      window.history.pushState(null, "", nextUrl);
    }
  }, [tab]);

  return (
    <div className="relative min-h-screen overflow-hidden bg-gradient-to-b from-[#3BAF87] to-[#9ED7C6] text-slate-900">
      <div className="pointer-events-none absolute bottom-0 left-0 h-28 w-full bg-gradient-to-t from-green-500/60 to-transparent" />
      <img
        src="/assets/sesac.png"
        alt=""
        aria-hidden="true"
        className="pointer-events-none fixed bottom-24 right-10 z-10 w-[240px] opacity-95 print:hidden"
      />
      <img
        src="/assets/sesac.png"
        alt=""
        aria-hidden="true"
        className="pointer-events-none fixed bottom-24 left-10 z-10 w-[240px] scale-x-[-1] opacity-95 print:hidden"
      />
      <div className="pointer-events-none fixed bottom-[40px] left-1/2 z-0 flex -translate-x-1/2 items-center justify-center gap-4 opacity-95 print:hidden">
        <img
          src="/assets/logo.png"
          alt="SeSAC logo"
          className="h-[150px] w-auto object-contain"
        />
        <img
          src="/assets/saltlux.png"
          alt="Saltluix logo"
          className="h-[70px] w-auto object-contain"
        />
      </div>

      <div className="relative z-20">
        <Topbar tab={tab} onTabChange={setTab} />
      </div>

      <main className="relative z-20 mx-auto w-full max-w-[1400px] px-4 py-5 pb-40 sm:px-6 lg:px-8 lg:py-6 lg:pb-48">
        {tab === "dashboard" && (
          <DashboardView
            dashboard={dashboard}
            traffic={traffic}
            topItems={topItems}
            topItemsVisible={topItemsVisible}
            risingItems={risingItems}
            showAllTop={showAllTop}
            onToggleShowAllTop={() => setShowAllTop((v) => !v)}
            formatNumber={formatNumber}
            formatPercent={formatPercent}
          />
        )}

        {tab === "reports" && (
          <ReportsView
            reportDate={reportDate}
            setReportDate={setReportDate}
            reportRange={reportRange}
            setReportRange={setReportRange}
            reports={reports}
            reportItems={reportItems}
            selectedReport={selectedReport}
            setSelectedReport={setSelectedReport}
            reportDetail={reportDetail}
            onRefreshReports={() => setReportsRefreshTick((v) => v + 1)}
          />
        )}
      </main>
    </div>
  );
}
