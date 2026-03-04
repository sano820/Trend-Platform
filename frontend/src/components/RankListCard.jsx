import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import AppCard from "@/components/ui/AppCard";

export default function RankListCard({
  title,
  pill,
  items,
  variant,
  className,
  formatNumber,
  formatPercent,
  showToggle,
  showAll,
  onToggle,
}) {
  return (
    <AppCard
      title={title}
      badge={
        <Badge
          variant="secondary"
          className="rounded-full border-slate-200 bg-slate-100 px-2.5 py-1 text-[11px] text-slate-700"
        >
          {pill}
        </Badge>
      }
      className={className}
      contentClassName="space-y-3"
    >
      {items.length === 0 && (
        <div className="rounded-lg border border-dashed border-slate-300 bg-slate-50 px-4 py-6 text-sm text-slate-600">
          데이터 없음
        </div>
      )}

      {items.length > 0 && (
        <div className="max-h-[420px] space-y-2 overflow-y-auto pr-1">
          {items.map((item) => {
            const key = variant === "top" ? item.rank || item.token : item.token;
            const barWidth =
              variant === "top"
                ? `${Math.min(100, (item.share || 0) * 1000)}%`
                : `${Math.min(100, (item.increase_rate || 0) * 40)}%`;
            const meta =
              variant === "top"
                ? `${formatNumber(item.count)} · 점유율 ${formatPercent(item.share)}`
                : `${formatNumber(item.prev_count)} → ${formatNumber(item.count)}`;
            const deltaCount =
              variant === "rising"
                ? Number(item.count) - Number(item.prev_count)
                : null;

            return (
              <div
                key={key}
                className="rounded-xl border border-slate-200 bg-white px-3 py-3 transition-all duration-200 hover:border-slate-300 hover:bg-slate-50"
              >
                <div className="grid grid-cols-[2rem_1fr_auto] items-center gap-3">
                  <div className="text-sm font-semibold text-slate-700">{item.rank || "-"}</div>
                  <div className="min-w-0">
                    <div className="truncate text-sm font-semibold text-slate-900">{item.token}</div>
                    <div className="truncate text-xs text-slate-600">
                      {meta}
                      {variant === "rising" && Number.isFinite(deltaCount) && (
                        <span className="ml-1 text-red-600">(+{formatNumber(deltaCount)})</span>
                      )}
                    </div>
                  </div>
                  <div className="text-xs font-semibold text-teal-700">
                    {item.is_new ? "NEW" : `${formatPercent(item.increase_rate)}`}
                  </div>
                </div>
                <div className="mt-2 h-1.5 overflow-hidden rounded-full bg-slate-200">
                  <span className="block h-full rounded-full bg-teal-600/70" style={{ width: barWidth }} />
                </div>
              </div>
            );
          })}
        </div>
      )}

      {showToggle && (
        <Button
          type="button"
          variant="outline"
          className="h-9 w-full rounded-xl border-slate-300 bg-white text-slate-800 transition-all duration-200 hover:bg-slate-50"
          onClick={onToggle}
        >
          {showAll ? "접기" : "더보기"}
        </Button>
      )}
    </AppCard>
  );
}
