import AppCard from "@/components/ui/AppCard";
import { TrafficSkeleton } from "@/components/Skeletons";

export default function TrafficCard({
  loading,
  error,
  traffic,
  trafficHistory,
  formatNumber,
}) {
  const recentTotal = Number(traffic?.total_posts ?? 0);
  const prevTotal = Number(traffic?.prev_total_posts ?? 0);
  const historyItems = Array.isArray(trafficHistory?.data?.items)
    ? trafficHistory.data.items
    : [];
  const historyValues = historyItems
    .map((item) => Number(item?.total_posts ?? 0))
    .filter((v) => Number.isFinite(v));
  const historyMin = historyValues.length ? Math.min(...historyValues) : 0;
  const historyMax = historyValues.length ? Math.max(...historyValues) : 1;
  const historyRange = historyMax - historyMin || 1;
  const sparkPoints =
    historyValues.length > 1
      ? historyValues
          .map((value, idx) => {
            const x = (idx / (historyValues.length - 1)) * 100;
            const y = 100 - ((value - historyMin) / historyRange) * 100;
            return `${x},${y}`;
          })
          .join(" ")
      : "";
  return (
    <AppCard
      title="최근 10분 트래픽"
      className="min-h-[500px] lg:col-span-2"
    >
      {loading && <TrafficSkeleton />}
      {error && (
        <div className="rounded-lg border border-dashed border-slate-300 bg-slate-50 px-4 py-6 text-sm text-slate-600">
          데이터를 불러오지 못했습니다.
        </div>
      )}
      {traffic && (
        <div className="space-y-6">
          <div className="grid gap-3 sm:grid-cols-3">
            <div className="rounded-xl border border-slate-200 bg-slate-50 p-4">
              <p className="text-xs text-slate-600">최근 10분</p>
              <p className="mt-1 text-2xl font-semibold tracking-tight text-slate-900">
                {formatNumber(traffic.total_posts)}
              </p>
            </div>
            <div className="rounded-xl border border-slate-200 bg-slate-50 p-4">
              <p className="text-xs text-slate-600">직전 10분</p>
              <p className="mt-1 text-2xl font-semibold tracking-tight text-slate-700">
                {formatNumber(traffic.prev_total_posts)}
              </p>
            </div>
            <div className="rounded-xl border border-slate-200 bg-slate-50 p-4">
              <p className="text-xs text-slate-600">증감률</p>
              <p
                className={`mt-1 text-2xl font-semibold tracking-tight ${
                  traffic.traffic_increase_rate >= 0 ? "text-teal-700" : "text-slate-700"
                }`}
              >
                {traffic.traffic_increase_rate === null
                  ? "-"
                  : `${traffic.traffic_increase_rate.toFixed(1)}%`}
              </p>
            </div>
          </div>

          <div className="rounded-xl border border-slate-200 bg-white/80 p-4">
            <div className="flex items-center justify-between text-xs font-semibold text-slate-700">
              <span>최근 트래픽 추이 (10분 단위)</span>
              <span className="text-slate-500">
                {historyValues.length ? `${historyValues.length}개` : "-"}
              </span>
            </div>
            <div className="mt-3">
              {historyValues.length > 1 ? (
                <svg
                  viewBox="0 0 100 100"
                  className="h-20 w-full"
                  role="img"
                  aria-label="10분 트래픽 스파크라인"
                  preserveAspectRatio="none"
                >
                  <defs>
                    <linearGradient id="trafficSpark" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="0%" stopColor="#14b8a6" stopOpacity="0.35" />
                      <stop offset="100%" stopColor="#14b8a6" stopOpacity="0.05" />
                    </linearGradient>
                  </defs>
                  <polyline
                    points={sparkPoints}
                    fill="none"
                    stroke="#0f766e"
                    strokeWidth="2"
                    strokeLinecap="round"
                    strokeLinejoin="round"
                  />
                  <polygon
                    points={`0,100 ${sparkPoints} 100,100`}
                    fill="url(#trafficSpark)"
                  />
                </svg>
              ) : (
                <div className="rounded-lg border border-dashed border-slate-200 bg-slate-50 px-3 py-4 text-center text-xs text-slate-500">
                  트래픽 히스토리가 아직 없습니다.
                </div>
              )}
              <div className="mt-3 flex items-center justify-between text-xs text-slate-600">
                <span>직전 10분: {formatNumber(prevTotal)}</span>
                <span>최근 10분: {formatNumber(recentTotal)}</span>
              </div>
            </div>
          </div>
        </div>
      )}
    </AppCard>
  );
}
