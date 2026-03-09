import { Sparkles, Tag, BarChart3, FolderTree } from "lucide-react";
import { motion } from "framer-motion";

const benefits = [
  {
    icon: Sparkles,
    title: "Standardized product names",
    desc: "Messy abbreviations become clean, readable entries.",
  },
  {
    icon: Tag,
    title: "Brand & pack size extraction",
    desc: "Buried metadata surfaced automatically from any format.",
  },
  {
    icon: BarChart3,
    title: "Quality scored",
    desc: "Every row rated 0–10 so you know where to focus.",
  },
  {
    icon: FolderTree,
    title: "Category taxonomy",
    desc: "Consistent 3-level product categorization applied.",
  },
];

const beforeAfterRows = [
  {
    product: { before: "PORK LOINS BONELESS CENTER CUT REF", after: "Pork Loins Boneless Center Cut Refrigerated" },
    brand: { before: "(buried in desc)", after: "Hatfield" },
    pack: { before: "(encoded)", after: "5 x 8 LB" },
    score: { before: "—", after: "9/10" },
  },
  {
    product: { before: "CHKN BRST BNLS SKNLS FRZ 4OZ", after: "Chicken Breast Boneless Skinless Frozen 4oz" },
    brand: { before: "(missing)", after: "Tyson" },
    pack: { before: "(unclear)", after: "2 x 10 LB" },
    score: { before: "—", after: "8/10" },
  },
];

const BenefitsSection = () => {
  return (
    <section className="py-20 lg:py-28 bg-background">
      <div className="container mx-auto px-4">
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          whileInView={{ opacity: 1, y: 0 }}
          viewport={{ once: true }}
          className="text-center mb-14"
        >
          <h2 className="font-heading text-3xl md:text-4xl font-bold text-foreground">
            What you get
          </h2>
          <p className="mt-3 text-muted-foreground text-lg max-w-md mx-auto">
            Your catalog, transformed in minutes
          </p>
        </motion.div>

        <div className="grid sm:grid-cols-2 lg:grid-cols-4 gap-5 mb-16">
          {benefits.map((b, i) => (
            <motion.div
              key={b.title}
              initial={{ opacity: 0, y: 20 }}
              whileInView={{ opacity: 1, y: 0 }}
              viewport={{ once: true }}
              transition={{ delay: i * 0.1 }}
              className="bg-card border border-border rounded-xl p-6 hover:shadow-lg hover:border-primary/20 transition-all duration-300"
            >
              <div className="w-10 h-10 rounded-lg bg-accent flex items-center justify-center mb-4">
                <b.icon className="w-5 h-5 text-accent-foreground" />
              </div>
              <h3 className="font-heading font-semibold text-foreground mb-1">{b.title}</h3>
              <p className="text-sm text-muted-foreground">{b.desc}</p>
            </motion.div>
          ))}
        </div>

        <motion.div
          initial={{ opacity: 0, y: 20 }}
          whileInView={{ opacity: 1, y: 0 }}
          viewport={{ once: true }}
          className="max-w-4xl mx-auto"
        >
          <h3 className="font-heading text-xl font-semibold text-foreground text-center mb-6">
            Before → After
          </h3>
          <div className="overflow-x-auto rounded-xl border border-border bg-card">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-border bg-muted/50">
                  <th className="text-left px-4 py-3 font-medium text-muted-foreground">Field</th>
                  <th className="text-left px-4 py-3 font-medium text-destructive/70">Before</th>
                  <th className="text-left px-4 py-3 font-medium text-primary">After</th>
                </tr>
              </thead>
              <tbody>
                {beforeAfterRows.map((row, i) => {
                  const fields = [
                    { label: "Product Name", ...row.product },
                    { label: "Brand", ...row.brand },
                    { label: "Pack/Size", ...row.pack },
                    { label: "Quality", ...row.score },
                  ];
                  return fields.map((cell, j) => (
                    <tr
                      key={`${i}-${j}`}
                      className={`border-b border-border/50 last:border-0 ${j === 0 && i > 0 ? "border-t-2 border-t-muted" : ""}`}
                    >
                      <td className="px-4 py-2.5 font-medium text-foreground">{cell.label}</td>
                      <td className="px-4 py-2.5 text-muted-foreground font-mono text-xs">{cell.before}</td>
                      <td className="px-4 py-2.5 text-foreground font-medium">{cell.after}</td>
                    </tr>
                  ));
                })}
              </tbody>
            </table>
          </div>
        </motion.div>
      </div>
    </section>
  );
};

export default BenefitsSection;
