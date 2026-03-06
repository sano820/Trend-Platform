import { Tabs, TabsList, TabsTrigger } from "@/components/ui/tabs";

export default function Topbar({ tab, onTabChange }) {
  return (
    <header className="sticky top-0 z-20 border-b border-white/40 bg-white/35 backdrop-blur-md print:hidden">
      <div className="mx-auto flex w-full max-w-[1400px] flex-col gap-4 px-4 py-4 sm:px-6 lg:flex-row lg:items-center lg:justify-between lg:px-8">
        <div className="flex items-center gap-3">
          <div>
            <h1 className="text-base font-semibold tracking-tight text-slate-900 sm:text-lg">
              Trend Platform
            </h1>
            <p className="text-xs text-slate-600">Realtime & Daily Insight</p>
          </div>
        </div>

        <Tabs value={tab} onValueChange={onTabChange}>
          <TabsList className="h-10 rounded-xl border border-slate-200 bg-slate-100/80 p-1 text-slate-600">
            <TabsTrigger
              value="dashboard"
              className="rounded-lg px-4 text-sm font-medium text-slate-700 transition-all duration-200 data-[state=active]:bg-white data-[state=active]:text-slate-900 data-[state=active]:shadow-sm"
            >
              실시간 대시보드
            </TabsTrigger>
            <TabsTrigger
              value="reports"
              className="rounded-lg px-4 text-sm font-medium text-slate-700 transition-all duration-200 data-[state=active]:bg-white data-[state=active]:text-slate-900 data-[state=active]:shadow-sm"
            >
              일일 보고서
            </TabsTrigger>
          </TabsList>
        </Tabs>
      </div>
    </header>
  );
}
