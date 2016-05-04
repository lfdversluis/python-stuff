from matplotlib.ticker import FuncFormatter
import matplotlib.pyplot as plt
import numpy as np

count = 0
x_labels = [] # Below axis
microseconds = [] # y values
colors = [] # color for the bar
hatches = ['/', "\\", "-", "+", "x", "o", "* ", "."]

with open("../data/benchmark_cpu_network_io.txt", "r") as file:
    for f in file.readlines():
        chunks = f.split(" ")
        cpu_blocking = chunks[0] == "True"
        net_blocking = chunks[1] == "True"
        io_blocking = chunks[2] == "True"

        # Create a combination color of the three boolean values!
        # 255 if non blocking else 0
        r = 255 if not cpu_blocking else 0
        g = 255 if not net_blocking else 0
        b = 255 if not io_blocking else 0
        color = '#%02x%02x%02x' % (r, g, b)
        colors.append(color)

        x_label = ""
        x_label += "b_cpu" if cpu_blocking else "nb_cpu"
        x_label += " b_net" if net_blocking else " nb_net"
        x_label += " b_io" if io_blocking else " nb_io"
        x_labels.append(x_label)

        microseconds.append(int(float((chunks[3]))))
        count += 1

x = np.arange(count)

microseconds, x_labels, colors = zip(*sorted(zip(microseconds, x_labels, colors), reverse=True))


def micro_to_milliseconds(x, pos):
    'The two args are the value and tick position'
    return '%1.1fms' % (x * 1e-3)


formatter = FuncFormatter(micro_to_milliseconds)

fig, ax = plt.subplots()
ax.yaxis.set_major_formatter(formatter)
barlist = plt.bar(x, microseconds)
plt.xticks(x + 0.5, x_labels)

# Set the bar colors and hatches
for i in range(len(colors)):
    barlist[i].set_hatch(hatches[i])
    barlist[i].set_color(colors[i])
    barlist[i].set_edgecolor("black")

for x, ms in zip(x, microseconds):
    plt.text(x+0.4, ms*1.01, '%1.1f' % (ms * 1e-3), ha="center")

# Set grey background color
ax.set_axis_bgcolor("#DDDDDD")
plt.show()
