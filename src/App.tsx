import { BrowserRouter, Route, Routes, Navigate, useParams } from "react-router-dom";
import { Toaster as Sonner } from "@/components/ui/sonner";
import { Toaster } from "@/components/ui/toaster";
import { TooltipProvider } from "@/components/ui/tooltip";
import Index from "./pages/Index.tsx";
import JobPage from "./pages/JobPage.tsx";
import NotFound from "./pages/NotFound.tsx";

/** Redirect legacy /processing/:jobId and /results/:jobId to /jobs/:jobId */
const LegacyRedirect = () => {
  const { jobId } = useParams();
  return <Navigate to={`/jobs/${jobId}`} replace />;
};

const App = () => (
  <TooltipProvider>
    <Toaster />
    <Sonner />
    <BrowserRouter>
      <Routes>
        <Route path="/" element={<Index />} />
        <Route path="/jobs/:jobId" element={<JobPage />} />
        {/* Legacy routes — redirect to unified /jobs/:jobId */}
        <Route path="/processing/:jobId" element={<LegacyRedirect />} />
        <Route path="/results/:jobId" element={<LegacyRedirect />} />
        <Route path="*" element={<NotFound />} />
      </Routes>
    </BrowserRouter>
  </TooltipProvider>
);

export default App;
