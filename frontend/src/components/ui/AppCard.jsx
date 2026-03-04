import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { cn } from "@/lib/utils";

export default function AppCard({
  title,
  badge,
  actions,
  className,
  contentClassName,
  children,
}) {
  return (
    <Card
      className={cn(
        "flex flex-col overflow-hidden rounded-2xl border border-white/40 bg-white/95 text-slate-900 shadow-lg backdrop-blur transition-all duration-200 hover:shadow-xl",
        className
      )}
    >
      {(title || badge || actions) && (
        <CardHeader className="flex flex-row items-start justify-between gap-3 space-y-0 pb-4">
          <div className="min-w-0">
            {title && (
              <CardTitle className="text-base font-semibold tracking-tight text-slate-900">
                {title}
              </CardTitle>
            )}
          </div>
          {(badge || actions) && (
            <div className="flex shrink-0 items-center gap-2">{badge}{actions}</div>
          )}
        </CardHeader>
      )}
      <CardContent className={cn("min-h-0 pt-0", contentClassName)}>{children}</CardContent>
    </Card>
  );
}
