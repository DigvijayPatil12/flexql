import pandas as pd
import matplotlib.pyplot as plt

# --- 1. Batch Size Plot ---
df_batch = pd.read_csv('batch_experiment.csv')
plt.figure(figsize=(10, 6))
plt.plot(df_batch['BatchSize'], df_batch['Throughput'], marker='o', color='purple', linewidth=2)
plt.xscale('log')
plt.title('Impact of Batch Size on Insertion Throughput', fontsize=14)
plt.xlabel('Batch Size (Log Scale)', fontsize=12)
plt.ylabel('Throughput (Rows / Second)', fontsize=12)
plt.grid(True, which='both', linestyle='--', alpha=0.7)
plt.tight_layout()
plt.savefig('plot_batch_size.png', dpi=300)

# --- 2. Scalability Plot ---
df_scale = pd.read_csv('benchmark_results.csv')
fig, ax1 = plt.subplots(figsize=(10, 6))

ax1.set_xlabel('Total Rows Inserted (Log Scale)', fontsize=12)
ax1.set_ylabel('Throughput (Rows / Sec)', color='tab:blue', fontsize=12)
ax1.plot(df_scale['Rows'], df_scale['Throughput_Rows_Sec'], marker='s', color='tab:blue', linewidth=2, label='Throughput')
ax1.tick_params(axis='y', labelcolor='tab:blue')
ax1.set_xscale('log')

ax2 = ax1.twinx()
ax2.set_ylabel('Total Execution Time (ms)', color='tab:red', fontsize=12)
ax2.plot(df_scale['Rows'], df_scale['Time_MS'], marker='^', color='tab:red', linestyle='--', linewidth=2, label='Time')
ax2.tick_params(axis='y', labelcolor='tab:red')

plt.title('Database Scalability: Up to 20 Million Rows', fontsize=14)
ax1.grid(True, which='both', linestyle='--', alpha=0.5)
fig.tight_layout()
plt.savefig('plot_scalability.png', dpi=300)

print("Plots generated: plot_batch_size.png and plot_scalability.png")