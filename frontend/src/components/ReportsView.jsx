import ReportDetail from "@/components/ReportDetail";
import ReportList from "@/components/ReportList";

export default function ReportsView({
  reportDate,
  setReportDate,
  reportRange,
  setReportRange,
  reports,
  reportItems,
  selectedDate,
  setSelectedDate,
  reportDetail,
}) {
  return (
    <section className="grid grid-cols-1 items-stretch gap-4 lg:grid-cols-[340px_1fr]">
      <div className="print:hidden">
        <ReportList
          reportDate={reportDate}
          setReportDate={setReportDate}
          reportRange={reportRange}
          setReportRange={setReportRange}
          loading={reports.loading}
          error={reports.error}
          reportItems={reportItems}
          selectedDate={selectedDate}
          setSelectedDate={setSelectedDate}
        />
      </div>

      <ReportDetail
        loading={reportDetail.loading}
        error={reportDetail.error}
        data={reportDetail.data}
        onPrint={() => window.print()}
      />
    </section>
  );
}
