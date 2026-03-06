export function ListSkeleton({ rows = 6 }) {
  return (
    <div className="space-y-3" aria-hidden="true">
      {Array.from({ length: rows }).map((_, idx) => (
        <div key={idx} className="h-12 animate-pulse rounded-lg bg-slate-200" />
      ))}
    </div>
  );
}

export function TrafficSkeleton() {
  return (
    <div className="space-y-4" aria-hidden="true">
      <div className="grid gap-3 sm:grid-cols-3">
        {Array.from({ length: 3 }).map((_, idx) => (
          <div key={idx} className="h-24 animate-pulse rounded-lg bg-slate-200" />
        ))}
      </div>
      <div className="h-4 w-40 animate-pulse rounded bg-slate-200" />
    </div>
  );
}

export function ReportDetailSkeleton() {
  return (
    <div className="space-y-3" aria-hidden="true">
      <div className="h-5 w-28 animate-pulse rounded bg-slate-200" />
      <div className="h-7 w-3/4 animate-pulse rounded bg-slate-200" />
      <div className="h-4 w-full animate-pulse rounded bg-slate-200" />
      <div className="h-4 w-5/6 animate-pulse rounded bg-slate-200" />
      <div className="h-4 w-2/3 animate-pulse rounded bg-slate-200" />
    </div>
  );
}
