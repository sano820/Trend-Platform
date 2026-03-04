import MarkdownLite from "@/components/MarkdownLite";
import { ReportDetailSkeleton } from "@/components/Skeletons";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import AppCard from "@/components/ui/AppCard";

export default function ReportDetail({ loading, error, data, onPrint }) {
  return (
    <AppCard
      title="보고서 상세"
      className="h-[550px] print:border-0 print:shadow-none"
      badge={
        <Badge
          variant="secondary"
          className="rounded-full border-slate-200 bg-slate-100 px-2.5 py-1 text-[11px] text-slate-700"
        >
          Report
        </Badge>
      }
      actions={
        <Button
          type="button"
          variant="outline"
          className="h-8 rounded-xl border-slate-300 bg-white px-3 text-xs text-slate-800 transition-all duration-200 hover:bg-slate-50"
          onClick={onPrint}
        >
          PDF 다운로드
        </Button>
      }
      contentClassName="flex h-full min-h-0 flex-col"
    >
      {loading && <ReportDetailSkeleton />}
      {error && (
        <div className="rounded-lg border border-dashed border-slate-300 bg-slate-50 px-4 py-6 text-sm text-slate-600">
          보고서를 불러오지 못했습니다.
        </div>
      )}

      {!loading && data && (
        <div className="min-h-0 flex-1 space-y-5 overflow-y-auto pr-1">
          <div className="space-y-2 border-b border-slate-200 pb-4">
            <p className="text-xs text-slate-600">{data.report_date}</p>
            <h3 className="text-xl font-semibold tracking-tight text-slate-900">
              {data.title || "Daily Trend Report"}
            </h3>
            {data.summary && <p className="text-sm text-slate-700">{data.summary}</p>}
            {Array.isArray(data.keywords) && data.keywords.length > 0 && (
              <div className="flex flex-wrap gap-2">
                {data.keywords.map((kw, idx) => (
                  <Badge
                    key={`${kw}-${idx}`}
                    variant="outline"
                    className="rounded-full border-slate-300 bg-slate-100 px-2.5 py-1 text-[11px] text-slate-700"
                  >
                    {kw}
                  </Badge>
                ))}
              </div>
            )}
          </div>
          <MarkdownLite content={data.content_md} />
        </div>
      )}
    </AppCard>
  );
}
