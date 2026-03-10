import {
  Accordion,
  AccordionContent,
  AccordionItem,
  AccordionTrigger,
} from "@/components/ui/accordion";
import { motion } from "framer-motion";

const faqs = [
  {
    q: "What CSV format do I need?",
    a: "Any format. Export from your ERP as-is. Any column names work — the AI figures out what each column contains.",
  },
  {
    q: "Is my data secure?",
    a: "Your catalog is processed on encrypted servers and automatically deleted within 24 hours. We never share your data.",
  },
  {
    q: "How long does it take?",
    a: "About 40 minutes for a typical catalog. Bookmark the results page and check back there when processing finishes.",
  },
  {
    q: "What does it cost?",
    a: "Completely free. No credit card, no commitment.",
  },
  {
    q: "What happens after?",
    a: "You download your enriched catalog. If you'd like help with product images or content, we'll show you how Pepper can help.",
  },
];

const FAQSection = () => {
  return (
    <section className="py-20 lg:py-28 bg-muted/30">
      <div className="container mx-auto px-4">
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          whileInView={{ opacity: 1, y: 0 }}
          viewport={{ once: true }}
          className="text-center mb-12"
        >
          <h2 className="font-heading text-3xl md:text-4xl font-bold text-foreground">
            Frequently asked questions
          </h2>
        </motion.div>

        <motion.div
          initial={{ opacity: 0, y: 20 }}
          whileInView={{ opacity: 1, y: 0 }}
          viewport={{ once: true }}
          className="max-w-2xl mx-auto"
        >
          <Accordion type="single" collapsible className="space-y-3">
            {faqs.map((faq, i) => (
              <AccordionItem
                key={i}
                value={`faq-${i}`}
                className="bg-card border border-border rounded-xl px-6 data-[state=open]:shadow-sm"
              >
                <AccordionTrigger className="text-left font-medium text-foreground hover:no-underline py-4">
                  {faq.q}
                </AccordionTrigger>
                <AccordionContent className="text-muted-foreground pb-4">
                  {faq.a}
                </AccordionContent>
              </AccordionItem>
            ))}
          </Accordion>
        </motion.div>
      </div>
    </section>
  );
};

export default FAQSection;
