import { motion } from "framer-motion";

const clients = [
  { name: "Prats", products: "42K" },
  { name: "CRS OneSource", products: "21K" },
  { name: "Doyles Sheehan", products: "19K" },
  { name: "Allen Paper Supply", products: "12K" },
];

const TrustBar = () => {
  return (
    <section className="bg-trust py-8 border-y border-border">
      <div className="container mx-auto px-4">
        <p className="text-xs uppercase tracking-widest text-muted-foreground text-center mb-6 font-medium">
          Powering catalogs for
        </p>
        <div className="flex flex-wrap items-center justify-center gap-8 md:gap-14">
          {clients.map((client, i) => (
            <motion.div
              key={client.name}
              initial={{ opacity: 0, y: 10 }}
              whileInView={{ opacity: 1, y: 0 }}
              viewport={{ once: true }}
              transition={{ delay: i * 0.1 }}
              className="flex flex-col items-center"
            >
              <div className="h-10 w-28 rounded-md bg-muted flex items-center justify-center">
                <span className="text-sm font-semibold text-foreground/70">{client.name}</span>
              </div>
              <span className="text-xs text-muted-foreground mt-1">{client.products}+ products</span>
            </motion.div>
          ))}
        </div>
        <motion.p
          initial={{ opacity: 0 }}
          whileInView={{ opacity: 1 }}
          viewport={{ once: true }}
          transition={{ delay: 0.5 }}
          className="text-center text-sm text-muted-foreground mt-6"
        >
          <span className="font-semibold text-foreground">94,000+</span> products cleaned and counting
        </motion.p>
      </div>
    </section>
  );
};

export default TrustBar;
