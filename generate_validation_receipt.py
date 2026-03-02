import re
from pathlib import Path

class ValidationReceiptGenerator:
    """
    @Data-Ops: Generates a validation receipt from session logs.
    """
    def __init__(self, log_path="logs/session.log", output_path="results/Grant_Validation_Receipt.txt"):
        self.log_path = Path(log_path)
        self.output_path = Path(output_path)
        
    def generate(self):
        if not self.log_path.exists():
            print(f"Log file not found: {self.log_path}")
            return

        with open(self.log_path, 'r', encoding='utf-8') as f:
            logs = f.readlines()
            
        total_sequences = 0
        latencies = []
        ntus = set()
        errors = 0
        
        # Parse logs for metrics
        for line in logs:
            if "Sequence Processed" in line:
                total_sequences += 1
                # Extract latency
                match = re.search(r"Latency: (\d+\.?\d*)ms", line)
                if match:
                    latencies.append(float(match.group(1)))
            
            if "New NTU Created" in line:
                # Extract NTU ID
                match = re.search(r"ID: (NTU_\d+)", line)
                if match:
                    ntus.add(match.group(1))
                    
            if "ERROR" in line or "CRITICAL" in line:
                errors += 1

        # Calculate metrics
        avg_latency = sum(latencies) / len(latencies) if latencies else 0.0
        stability = 100.0 if total_sequences > 0 and errors == 0 else (0.0 if total_sequences == 0 else max(0, 100 - (errors * 10)))

        # Format Requirement:
        # 'Total Sequences Processed: 15'
        # 'Mean Latency per Signature: 9.4ms'
        # 'Novel Taxonomic Units Found: 2'
        # 'System Stability: 100%'
        
        report = []
        report.append(f"Total Sequences Processed: {total_sequences}")
        report.append(f"Mean Latency per Signature: {avg_latency:.1f}ms")
        report.append(f"Novel Taxonomic Units Found: {len(ntus)}")
        report.append(f"System Stability: {int(stability)}%")
        
        # Write to file
        with open(self.output_path, 'w', encoding='utf-8') as f:
            f.write("\n".join(report))
            
        print(f"Validation Receipt generated at: {self.output_path}")
        print("\n".join(report))

if __name__ == "__main__":
    generator = ValidationReceiptGenerator()
    generator.generate()
