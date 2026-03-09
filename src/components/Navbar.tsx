import { Button } from "@/components/ui/button";

const Navbar = () => {
  return (
    <nav className="absolute top-0 left-0 right-0 z-50 py-4">
      <div className="container mx-auto px-4 flex items-center justify-between">
        <a href="/" className="font-heading text-xl font-bold text-hero-foreground">
          <span className="text-primary">Clean</span>Data
        </a>
        <Button variant="hero-secondary" size="sm">
          Contact
        </Button>
      </div>
    </nav>
  );
};

export default Navbar;
