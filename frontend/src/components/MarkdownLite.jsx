export default function MarkdownLite({ content }) {
  if (!content) {
    return (
      <div className="rounded-lg border border-dashed border-slate-300 bg-slate-50 px-4 py-6 text-sm text-slate-600">
        내용이 없습니다.
      </div>
    );
  }

  const lines = content.split("\n");

  return (
    <div className="space-y-2 text-[15px] leading-7 text-slate-800">
      {lines.map((line, idx) => {
        if (line.startsWith("# ")) {
          return (
            <h2 key={idx} className="pt-2 text-xl font-semibold tracking-tight text-slate-900">
              {line.replace(/^#\s+/, "")}
            </h2>
          );
        }
        if (line.startsWith("## ")) {
          return (
            <h3 key={idx} className="pt-1 text-lg font-semibold tracking-tight text-slate-900">
              {line.replace(/^##\s+/, "")}
            </h3>
          );
        }
        if (line.startsWith("- ")) {
          return (
            <div key={idx} className="flex items-start gap-2">
              <span className="mt-3 h-1.5 w-1.5 rounded-full bg-teal-600/80" />
              <span>{line.replace(/^-\s+/, "")}</span>
            </div>
          );
        }
        if (!line.trim()) {
          return <div key={idx} className="h-2" />;
        }
        return <p key={idx}>{line}</p>;
      })}
    </div>
  );
}
