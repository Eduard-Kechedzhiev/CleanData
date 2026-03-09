import { useState, useCallback } from "react";
import Navbar from "@/components/Navbar";
import HeroSection from "@/components/HeroSection";
import TrustBar from "@/components/TrustBar";
import BenefitsSection from "@/components/BenefitsSection";
import HowItWorks from "@/components/HowItWorks";
import SampleResults from "@/components/SampleResults";
import BenchmarksSection from "@/components/BenchmarksSection";
import FAQSection from "@/components/FAQSection";
import FinalCTA from "@/components/FinalCTA";
import ProcessingPage from "@/components/ProcessingPage";
import ResultsPage from "@/components/ResultsPage";

type AppStep = "landing" | "processing" | "results";

const Index = () => {
  const [step, setStep] = useState<AppStep>("landing");
  const [capturedEmail, setCapturedEmail] = useState<string | null>(null);

  const handleUpload = useCallback((_file: File) => {
    setStep("processing");
    window.scrollTo({ top: 0 });
  }, []);

  const handleSampleData = useCallback(() => {
    const el = document.getElementById("sample-results");
    if (el) el.scrollIntoView({ behavior: "smooth" });
  }, []);

  const handleProcessingComplete = useCallback(() => {
    setStep("results");
    window.scrollTo({ top: 0 });
  }, []);

  const handleEmailCapture = useCallback((email: string) => {
    setCapturedEmail(email);
  }, []);

  if (step === "processing") {
    return (
      <ProcessingPage
        onComplete={handleProcessingComplete}
        onEmailCapture={handleEmailCapture}
        capturedEmail={capturedEmail}
      />
    );
  }

  if (step === "results") {
    return (
      <ResultsPage
        capturedEmail={capturedEmail}
        onEmailCapture={handleEmailCapture}
      />
    );
  }

  return (
    <div className="min-h-screen">
      <Navbar />
      <HeroSection onUpload={handleUpload} onSampleData={handleSampleData} />
      <TrustBar />
      <BenefitsSection />
      <HowItWorks />
      <SampleResults onTrySample={() => window.scrollTo({ top: 0, behavior: "smooth" })} />
      <BenchmarksSection />
      <FAQSection />
      <FinalCTA />
    </div>
  );
};

export default Index;
