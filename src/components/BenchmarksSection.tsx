import { motion } from "framer-motion";

const metrics = [
  { label: "Names cleaned", value: "100%", color: "text-primary" },
  { label: "Brands extracted", value: "99.7%", color: "text-primary" },
  { label: "Avg quality score", value: "8.4 / 10", color: "text-score-excellent" },
  { label: "Excellent + Good ratings", value: "98.8%", color: "text-primary" },
  { label: "Taxonomy assigned", value: "93.3%", color: "text-primary" },
];

const BenchmarksSection = () => {
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
            Proven results
          </h2>
          <p className="mt-3 text-muted-foreground text-lg max-w-lg mx-auto">
            Benchmarked on a real 8,863-product broadline distributor catalog
          </p>
        </motion.div>

        <div className="max-w-3xl mx-auto grid grid-cols-2 md:grid-cols-5 gap-4">
          {metrics.map((m, i) => (
            <motion.div
              key={m.label}
              initial={{ opacity: 0, scale: 0.9 }}
              whileInView={{ opacity: 1, scale: 1 }}
              viewport={{ once: true }}
              transition={{ delay: i * 0.08 }}
              className="bg-card border border-border rounded-xl p-5 text-center hover:shadow-md transition-shadow"
            >
              <p className={`text-2xl md:text-3xl font-heading font-bold ${m.color}`}>
                {m.value}
              </p>
              <p className="text-xs text-muted-foreground mt-1 leading-tight">{m.label}</p>
            </motion.div>
          ))}
        </div>
      </div>
    </section>
  );
};

export default BenchmarksSection;
