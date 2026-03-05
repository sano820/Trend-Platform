import RankListCard from "@/components/RankListCard";
import TrafficCard from "@/components/TrafficCard";

export default function DashboardView({
  dashboard,
  traffic,
  trafficHistory,
  topItems,
  topItemsVisible,
  risingItems,
  showAllTop,
  onToggleShowAllTop,
  formatNumber,
  formatPercent,
}) {
  return (
    <section className="grid grid-cols-1 items-stretch gap-4 md:grid-cols-2 xl:grid-cols-4">
      <TrafficCard
        loading={dashboard.loading}
        error={dashboard.error}
        traffic={traffic}
        trafficHistory={trafficHistory}
        formatNumber={formatNumber}
      />

      <RankListCard
        title="실시간 Top 단어"
        pill="Top 20"
        variant="top"
        className="min-h-[500px]"
        items={topItemsVisible}
        showToggle={topItems.length > 10}
        showAll={showAllTop}
        onToggle={onToggleShowAllTop}
        formatNumber={formatNumber}
        formatPercent={formatPercent}
      />

      <RankListCard
        title="급상승 단어"
        pill="Rising"
        variant="rising"
        className="min-h-[500px]"
        items={risingItems.slice(0, 10)}
        formatNumber={formatNumber}
        formatPercent={formatPercent}
      />
    </section>
  );
}
