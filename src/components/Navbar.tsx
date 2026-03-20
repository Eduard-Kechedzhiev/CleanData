import { Button } from "@/components/ui/button";
import { Link } from "react-router-dom";

const CONTACT_EMAIL = "hello@cleandata.com";

const Navbar = () => {
  return (
    <nav className="absolute top-0 left-0 right-0 z-50 py-4">
      <div className="container mx-auto px-4 flex items-center justify-between">
        <Link to="/" className="font-heading text-xl font-bold text-hero-foreground">
          <span className="text-primary">Clean</span>Data
        </Link>
        <a href={`mailto:${CONTACT_EMAIL}`}>
          <Button variant="hero-secondary" size="sm">
            Contact
          </Button>
        </a>
      </div>
    </nav>
  );
};

export default Navbar;
