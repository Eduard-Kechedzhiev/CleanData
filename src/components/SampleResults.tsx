import { motion } from "framer-motion";
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Cell } from "recharts";
import { Button } from "@/components/ui/button";

interface SampleResultsProps {
  onTrySample: () => void;
}

const sampleRows = [
  { before: "PORK LOINS BNLS CNTR CUT REF", after: "Pork Loins Boneless Center Cut Refrigerated", brand: "Hatfield", score: 9 },
  { before: "CHKN BRST BNLS SKNLS FRZ 4OZ", after: "Chicken Breast Boneless Skinless Frozen 4oz", brand: "Tyson", score: 8 },
  { before: "PPR TOWEL 2PLY CS/30", after: "Paper Towel 2-Ply Case of 30", brand: "Bounty", score: 7 },
  { before: "MAYO PKT .44OZ 200CT", after: "Mayonnaise Packet 0.44oz 200 Count", brand: "Heinz", score: 9 },
  { before: "FRZ FRIES CRINKLE 5LB", after: "Frozen French Fries Crinkle Cut 5lb", brand: "McCain", score: 8 },
  { before: "BEF PATTY 80/20 6OZ FRZ", after: "Beef Patty 80/20 6oz Frozen", brand: "Cargill", score: 8 },
];

const scoreDistribution = [
  { score: "1-2", count: 12, color: "hsl(var(--score-poor))" },
  { score: "3-4", count: 45, color: "hsl(var(--score-poor))" },
  { score: "5-6", count: 198, color: "hsl(var(--score-good))" },
  { score: "7-8", count: 3842, color: "hsl(var(--score-excellent))" },
  { score: "9-10", count: 4766, color: "hsl(var(--score-excellent))" },
];

const scoreColor = (s: number) =>
  s >= 8 ? "text-score-excellent" : s >= 5 ? "text-score-good" : "text-score-poor";

const SampleResults = ({ onTrySample }: SampleResultsProps) => {
  return (
    <section id="sample-results" className="py-20 lg:py-28 bg-background">
      <div className="container mx-auto px-4">
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          whileInView={{ opacity: 1, y: 0 }}
          viewport={{ once: true }}
          className="text-center mb-12"
        >
          <h2 className="font-heading text-3xl md:text-4xl font-bold text-foreground">
            See it in action
          </h2>
          <p className="mt-3 text-muted-foreground text-lg max-w-lg mx-auto">
            Real results from a broadline distributor catalog
          </p>
        </motion.div>

        {/* Sample table */}
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          whileInView={{ opacity: 1, y: 0 }}
          viewport={{ once: true }}
          className="max-w-4xl mx-auto mb-10"
        >
          <div className="overflow-x-auto rounded-xl border border-border bg-card">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-border bg-muted/50">
                  <th className="text-left px-4 py-3 font-medium text-muted-foreground">Original</th>
                  <th className="text-left px-4 py-3 font-medium text-muted-foreground">Cleaned</th>
                  <th className="text-left px-4 py-3 font-medium text-muted-foreground">Brand</th>
                  <th className="text-center px-4 py-3 font-medium text-muted-foreground">Score</th>
                </tr>
              </thead>
              <tbody>
                {sampleRows.map((row, i) => (
                  <tr key={i} className="border-b border-border/50 last:border-0">
                    <td className="px-4 py-3 text-muted-foreground font-mono text-xs">{row.before}</td>
                    <td className="px-4 py-3 text-foreground font-medium">{row.after}</td>
                    <td className="px-4 py-3 text-foreground">{row.brand}</td>
                    <td className={`px-4 py-3 text-center font-bold ${scoreColor(row.score)}`}>{row.score}/10</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </motion.div>

        {/* Chart */}
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          whileInView={{ opacity: 1, y: 0 }}
          viewport={{ once: true }}
          className="max-w-xl mx-auto bg-card border border-border rounded-xl p-6 mb-8"
        >
          <h3 className="font-heading font-semibold text-foreground mb-4 text-center">Quality Score Distribution</h3>
          <ResponsiveContainer width="100%" height={200}>
            <BarChart data={scoreDistribution}>
              <XAxis dataKey="score" tick={{ fontSize: 12, fill: "hsl(var(--muted-foreground))" }} axisLine={false} tickLine={false} />
              <YAxis tick={{ fontSize: 12, fill: "hsl(var(--muted-foreground))" }} axisLine={false} tickLine={false} />
              <Tooltip
                contentStyle={{ background: "hsl(var(--card))", border: "1px solid hsl(var(--border))", borderRadius: 8, fontSize: 13 }}
              />
              <Bar dataKey="count" radius={[6, 6, 0, 0]}>
                {scoreDistribution.map((entry, i) => (
                  <Cell key={i} fill={entry.color} />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </motion.div>

        <div className="text-center">
          <Button variant="cta" size="lg" onClick={onTrySample}>
            Try with your own data
          </Button>
        </div>
      </div>
    </section>
  );
};

export default SampleResults;
