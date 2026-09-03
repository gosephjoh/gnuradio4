import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from scipy import stats

def analyze_and_plot_profiling(csv_path="gr4_all_blocks_profiling.csv", output_dir="block_profiling_plots"):
    if not os.path.exists(csv_path):
        print(f"Error: {csv_path} not found!")
        return

    os.makedirs(output_dir, exist_ok=True)
    df = pd.read_csv(csv_path)

    blocks = df['Block_Name'].unique()

    summary_rows = []

    plt.figure(figsize=(10, 6))
    
    # Custom color palette
    colors = plt.cm.tab10(np.linspace(0, 1, len(blocks)))

    fig_all, ax_all = plt.subplots(figsize=(10, 6), dpi=300)

    for idx, block_name in enumerate(blocks):
        block_df = df[df['Block_Name'] == block_name].sort_values('Sample_Size')
        
        x = block_df['Sample_Size'].values
        y = block_df['Avg_Time_us'].values

        # Perform linear regression: y = m * x + c
        slope, intercept, r_value, p_value, std_err = stats.linregress(x, y)
        r_squared = r_value ** 2

        # Cost metrics
        ns_per_sample = slope * 1000.0  # us to ns
        msps_throughput = 1.0 / slope if slope > 0 else 0.0

        summary_rows.append({
            'Block Name': block_name,
            'Slope (ns/sample)': ns_per_sample,
            'Intercept (us)': intercept,
            'R2 Score': r_squared,
            'Throughput (MSps)': msps_throughput
        })

        # --- Individual Plot per Block ---
        fig, ax = plt.subplots(figsize=(8, 5), dpi=300)
        
        # Data points
        ax.scatter(x / 1e6, y / 1e3, color=colors[idx], label='Measured Data', s=40, zorder=3)
        
        # Regression line
        x_fit = np.linspace(x.min(), x.max(), 100)
        y_fit = slope * x_fit + intercept
        ax.plot(x_fit / 1e6, y_fit / 1e3, color='crimson', linestyle='--', linewidth=2, label='Linear Fit')

        ax.set_title(f"GNU Radio 4.0 Profiling: {block_name}", fontsize=14, fontweight='bold', pad=12)
        ax.set_xlabel("Samples Processed (Millions)", fontsize=11)
        ax.set_ylabel("Execution Time (ms)", fontsize=11)
        ax.grid(True, linestyle=':', alpha=0.6)

        # Annotation Box
        info_text = (
            f"Fit Equation: y = {slope:.6f}x + {intercept:.2f}\n"
            f"Cost per Sample: {ns_per_sample:.2f} ns/sample\n"
            f"Overhead (c): {intercept:.2f} µs\n"
            f"R² Score: {r_squared:.4f}\n"
            f"Throughput: {msps_throughput:.2f} MSps"
        )
        ax.text(0.05, 0.95, info_text, transform=ax.transAxes, fontsize=10,
                verticalalignment='top', bbox=dict(boxstyle='round,pad=0.5', facecolor='white', alpha=0.85, edgecolor='gray'))

        ax.legend(loc='lower right')
        plt.tight_layout()
        
        # Clean filename
        safe_name = block_name.replace('<', '_').replace('>', '_').replace(':', '_')
        plot_path = os.path.join(output_dir, f"{safe_name}_regression.png")
        plt.savefig(plot_path)
        plt.close(fig)

        # Add to combined multi-line plot
        ax_all.plot(x / 1e6, y / 1e3, marker='o', linewidth=2, label=f"{block_name} ({msps_throughput:.1f} MSps)")

    # Complete Combined Multi-Line Plot
    ax_all.set_title("GNU Radio 4.0 Block Execution Time Comparison", fontsize=14, fontweight='bold', pad=12)
    ax_all.set_xlabel("Samples Processed (Millions)", fontsize=11)
    ax_all.set_ylabel("Execution Time (ms)", fontsize=11)
    ax_all.grid(True, linestyle=':', alpha=0.6)
    ax_all.legend(loc='upper left', fontsize=10)
    fig_all.tight_layout()
    fig_all.savefig(os.path.join(output_dir, "all_blocks_regression_comparison.png"))
    plt.close(fig_all)

    # --- Bar Chart Comparison of Throughput ---
    summary_df = pd.DataFrame(summary_rows)
    
    fig_bar, ax_bar = plt.subplots(figsize=(10, 6), dpi=300)
    bars = ax_bar.bar(summary_df['Block Name'], summary_df['Throughput (MSps)'], color=colors[:len(blocks)], edgecolor='black', alpha=0.85)
    
    ax_bar.set_title("GNU Radio 4.0 Built-in Block Throughput Comparison", fontsize=14, fontweight='bold', pad=12)
    ax_bar.set_ylabel("Throughput (MSps)", fontsize=11)
    ax_bar.grid(axis='y', linestyle=':', alpha=0.6)
    plt.xticks(rotation=15, ha='right', fontsize=10)

    for bar in bars:
        yval = bar.get_height()
        ax_bar.text(bar.get_x() + bar.get_width()/2.0, yval + 0.5, f"{yval:.2f} MSps", ha='center', va='bottom', fontsize=9, fontweight='bold')

    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "summary_throughput_bar_chart.png"))
    plt.close(fig_bar)

    # Output Summary Table to Console
    print("\n=========================================================================================================")
    print("  GNU RADIO 4.0 BUILT-IN BLOCKS LINEAR REGRESSION SUMMARY RESULTS  ")
    print("=========================================================================================================")
    print(f"{'Block Name':<28} | {'Slope (ns/sample)':<18} | {'Intercept (us)':<14} | {'R2 Score':<10} | {'Throughput (MSps)':<18}")
    print("-" * 105)
    for _, row in summary_df.iterrows():
        print(f"{row['Block Name']:<28} | {row['Slope (ns/sample)']:<18.3f} | {row['Intercept (us)']:<14.2f} | {row['R2 Score']:<10.4f} | {row['Throughput (MSps)']:<18.2f}")
    print("=========================================================================================================\n")
    print(f"All plots successfully generated in directory: '{output_dir}/'")

if __name__ == "__main__":
    analyze_and_plot_profiling()
