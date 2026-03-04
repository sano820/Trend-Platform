import AppCard from "@/components/ui/AppCard";
import { TrafficSkeleton } from "@/components/Skeletons";

export default function TrafficCard({ loading, error, traffic, formatNumber }) {
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
        <div className="space-y-4">
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
        </div>
      )}
    </AppCard>
  );
}
