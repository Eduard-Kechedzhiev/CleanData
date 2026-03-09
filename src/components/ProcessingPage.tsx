import { useState, useEffect } from "react";
import { Check, Loader2, Mail } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import Stepper from "./Stepper";
import { motion } from "framer-motion";

interface ProcessingPageProps {
  onComplete: () => void;
  onEmailCapture: (email: string) => void;
  capturedEmail: string | null;
}

const stages = [
  { label: "Reading your catalog...", duration: 2000 },
  { label: "Cleaning product names...", duration: 3000, count: true },
  { label: "Extracting brands and pack sizes...", duration: 3000 },
  { label: "Scoring quality...", duration: 2500 },
  { label: "Assigning categories...", duration: 2500 },
  { label: "Validating GTINs...", duration: 2000 },
];

const totalProducts = 8863;

const ProcessingPage = ({ onComplete, onEmailCapture, capturedEmail }: ProcessingPageProps) => {
  const [currentStage, setCurrentStage] = useState(0);
  const [progress, setProgress] = useState(0);
  const [email, setEmail] = useState("");
  const [emailSubmitted, setEmailSubmitted] = useState(!!capturedEmail);

  useEffect(() => {
    const totalDuration = stages.reduce((sum, s) => sum + s.duration, 0);
    let elapsed = 0;

    const timers: NodeJS.Timeout[] = [];

    stages.forEach((stage, i) => {
      timers.push(
        setTimeout(() => setCurrentStage(i), elapsed)
      );
      elapsed += stage.duration;
    });

    // Progress bar
    const interval = setInterval(() => {
      setProgress((prev) => {
        const next = prev + (100 / (totalDuration / 100));
        if (next >= 100) {
          clearInterval(interval);
          setTimeout(onComplete, 500);
          return 100;
        }
        return next;
      });
    }, 100);

    return () => {
      timers.forEach(clearTimeout);
      clearInterval(interval);
    };
  }, [onComplete]);

  const handleEmailSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (email.includes("@")) {
      onEmailCapture(email);
      setEmailSubmitted(true);
    }
  };

  return (
    <div className="min-h-screen bg-background flex flex-col">
      <div className="container mx-auto px-4 py-6">
        <a href="/" className="font-heading text-xl font-bold text-foreground">
          <span className="text-primary">Clean</span>Data
        </a>
      </div>

      <div className="container mx-auto px-4 py-4">
        <Stepper currentStep={2} />
      </div>

      <div className="flex-1 flex items-center justify-center px-4 pb-20">
        <div className="max-w-md w-full">
          <motion.div
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            className="text-center"
          >
            <div className="mb-8">
              <Loader2 className="w-12 h-12 text-primary mx-auto animate-spin" />
            </div>

            <h1 className="font-heading text-2xl font-bold text-foreground mb-2">
              Processing your catalog
            </h1>

            {/* Progress bar */}
            <div className="w-full bg-muted rounded-full h-2 mb-6 overflow-hidden">
              <motion.div
                className="h-full bg-primary rounded-full"
                initial={{ width: 0 }}
                animate={{ width: `${progress}%` }}
                transition={{ ease: "linear" }}
              />
            </div>

            {/* Stages */}
            <div className="space-y-2 mb-10 text-left">
              {stages.map((stage, i) => (
                <div
                  key={i}
                  className={`flex items-center gap-2 text-sm transition-opacity ${
                    i < currentStage
                      ? "text-primary"
                      : i === currentStage
                      ? "text-foreground font-medium"
                      : "text-muted-foreground/40"
                  }`}
                >
                  {i < currentStage ? (
                    <Check className="w-4 h-4 text-primary flex-shrink-0" />
                  ) : i === currentStage ? (
                    <Loader2 className="w-4 h-4 animate-spin flex-shrink-0" />
                  ) : (
                    <div className="w-4 h-4 rounded-full border border-current flex-shrink-0" />
                  )}
                  <span>{stage.label}</span>
                  {stage.count && i === currentStage && (
                    <span className="text-xs text-muted-foreground ml-auto">
                      {Math.min(Math.round(progress * totalProducts / 100), totalProducts).toLocaleString()} / {totalProducts.toLocaleString()}
                    </span>
                  )}
                </div>
              ))}
            </div>

            {/* Email capture */}
            {!emailSubmitted ? (
              <motion.div
                initial={{ opacity: 0, y: 10 }}
                animate={{ opacity: 1, y: 0 }}
                transition={{ delay: 1 }}
                className="bg-card border border-border rounded-xl p-6"
              >
                <Mail className="w-8 h-8 text-primary mx-auto mb-3" />
                <p className="font-medium text-foreground mb-1">
                  This takes about 40 minutes
                </p>
                <p className="text-sm text-muted-foreground mb-4">
                  We'll email you the moment it's ready.
                </p>
                <form onSubmit={handleEmailSubmit} className="flex gap-2">
                  <Input
                    type="email"
                    placeholder="you@company.com"
                    value={email}
                    onChange={(e) => setEmail(e.target.value)}
                    className="flex-1"
                  />
                  <Button type="submit" variant="default" size="default">
                    Notify Me
                  </Button>
                </form>
              </motion.div>
            ) : (
              <motion.div
                initial={{ opacity: 0, scale: 0.95 }}
                animate={{ opacity: 1, scale: 1 }}
                className="bg-accent border border-primary/20 rounded-xl p-6 text-center"
              >
                <Check className="w-8 h-8 text-primary mx-auto mb-2" />
                <p className="font-medium text-foreground">We'll notify you when it's ready!</p>
                <p className="text-sm text-muted-foreground mt-1">You can safely close this page.</p>
              </motion.div>
            )}
          </motion.div>
        </div>
      </div>
    </div>
  );
};

export default ProcessingPage;
