import { useState, useCallback } from "react";
import { useNavigate } from "react-router-dom";
import Navbar from "@/components/Navbar";
import HeroSection from "@/components/HeroSection";
import BenefitsSection from "@/components/BenefitsSection";
import SampleResults from "@/components/SampleResults";
import BenchmarksSection from "@/components/BenchmarksSection";
import FAQSection from "@/components/FAQSection";
import FinalCTA from "@/components/FinalCTA";
import { uploadFile } from "@/lib/api";
import { useToast } from "@/hooks/use-toast";

const Index = () => {
  const navigate = useNavigate();
  const { toast } = useToast();
  const [uploading, setUploading] = useState(false);

  const handleUpload = useCallback(async (file: File) => {
    setUploading(true);
    try {
      const res = await uploadFile(file);
      navigate(`/jobs/${res.job_id}`);
    } catch (err: any) {
      toast({
        title: "Upload failed",
        description: err.message || "Something went wrong. Please try again.",
        variant: "destructive",
      });
      setUploading(false);
    }
  }, [navigate, toast]);

  const handleSampleData = useCallback(() => {
    const el = document.getElementById("sample-results");
    if (el) el.scrollIntoView({ behavior: "smooth" });
  }, []);

  return (
    <div className="min-h-screen">
      <Navbar />
      <HeroSection onUpload={handleUpload} onSampleData={handleSampleData} uploading={uploading} />
      <BenefitsSection />
      <SampleResults onTrySample={() => window.scrollTo({ top: 0, behavior: "smooth" })} />
      <BenchmarksSection />
      <FAQSection />
      <FinalCTA />
    </div>
  );
};

export default Index;
