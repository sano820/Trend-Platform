import AppCard from "@/components/ui/AppCard";
import { TrafficSkeleton } from "@/components/Skeletons";

export default function TrafficCard({ loading, error, traffic, formatNumber }) {
  const recentTotal = Number(traffic?.total_posts ?? 0);
  const prevTotal = Number(traffic?.prev_total_posts ?? 0);
  const maxTotal = Math.max(recentTotal, prevTotal, 1);
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
            <div className="text-xs font-semibold text-slate-700">10분 비교 그래프</div>
            <div className="mt-3 space-y-3">
              {[
                { label: "최근 10분", value: recentTotal, bar: "bg-teal-500" },
                { label: "직전 10분", value: prevTotal, bar: "bg-slate-400" },
              ].map((row) => {
                const width = Math.round((row.value / maxTotal) * 100);
                return (
                  <div key={row.label} className="space-y-1">
                    <div className="flex items-center justify-between text-xs text-slate-600">
                      <span>{row.label}</span>
                      <span>{formatNumber(row.value)}</span>
                    </div>
                    <div className="h-2 w-full rounded-full bg-slate-100">
                      <div
                        className={`h-2 rounded-full ${row.bar}`}
                        style={{ width: `${width}%` }}
                        aria-label={`${row.label} ${row.value}`}
                      />
                    </div>
                  </div>
                );
              })}
            </div>
          </div>
        </div>
      )}
    </AppCard>
  );
}
