import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import AppCard from "@/components/ui/AppCard";
import { ListSkeleton } from "@/components/Skeletons";

export default function ReportList({
  reportDate,
  setReportDate,
  reportRange,
  setReportRange,
  loading,
  error,
  reportItems,
  selectedDate,
  setSelectedDate,
}) {
  return (
    <AppCard
      title="보고서 리스트"
      badge={
        <Badge
          variant="secondary"
          className="rounded-full border-slate-200 bg-slate-100 px-2.5 py-1 text-[11px] text-slate-700"
        >
          Daily
        </Badge>
      }
      className="h-[550px]"
      contentClassName="flex h-full min-h-0 flex-col gap-4"
    >
      <div className="space-y-2">
        <Input
          type="date"
          value={reportDate}
          onChange={(e) => setReportDate(e.target.value)}
          className="h-10 border-slate-300 bg-white text-slate-900 focus-visible:ring-teal-500"
        />
        <div className="flex items-center gap-2">
          <Button
            type="button"
            variant="outline"
            size="sm"
            className={`rounded-full border transition-all duration-200 ${
              reportRange === "all"
                ? "border-teal-600 bg-teal-600 text-white hover:bg-teal-700 hover:text-white"
                : "border-slate-300 bg-white text-slate-700 hover:bg-slate-50"
            }`}
            onClick={() => setReportRange("all")}
          >
            전체
          </Button>
          <Button
            type="button"
            variant="outline"
            size="sm"
            className={`rounded-full border transition-all duration-200 ${
              reportRange === "7"
                ? "border-teal-600 bg-teal-600 text-white hover:bg-teal-700 hover:text-white"
                : "border-slate-300 bg-white text-slate-700 hover:bg-slate-50"
            }`}
            onClick={() => setReportRange("7")}
          >
            7일
          </Button>
          <Button
            type="button"
            variant="outline"
            size="sm"
            className={`rounded-full border transition-all duration-200 ${
              reportRange === "30"
                ? "border-teal-600 bg-teal-600 text-white hover:bg-teal-700 hover:text-white"
                : "border-slate-300 bg-white text-slate-700 hover:bg-slate-50"
            }`}
            onClick={() => setReportRange("30")}
          >
            30일
          </Button>
        </div>
      </div>

      {loading && <ListSkeleton rows={6} />}
      {error && (
        <div className="rounded-lg border border-dashed border-slate-300 bg-slate-50 px-4 py-6 text-sm text-slate-600">
          목록을 불러오지 못했습니다.
        </div>
      )}

      <div className="min-h-0 flex-1 space-y-2 overflow-y-auto pr-1">
        {reportItems.map((item) => (
          <button
            key={item.report_date}
            type="button"
            className={`w-full rounded-xl border px-3 py-3 text-left transition-all duration-200 ${
              selectedDate === item.report_date
                ? "border-teal-500/70 bg-slate-100"
                : "border-slate-200 bg-white hover:border-slate-300 hover:bg-slate-50"
            }`}
            onClick={() => setSelectedDate(item.report_date)}
          >
            <div className="text-xs text-slate-600">{item.report_date}</div>
            <div className="mt-1 truncate text-sm font-semibold text-slate-900">
              {item.title || "리포트"}
            </div>
          </button>
        ))}
      </div>
    </AppCard>
  );
}
