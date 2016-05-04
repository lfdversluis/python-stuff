import os

import rpy2.robjects as robjects
import subprocess

class LatencyBenchmarkDataParser:
    """
    Calculated the latency for the small task of the latency_benchmark_small_task job.
    Generates and executes a graph in R format.
    """

    def __init__(self):
        self.base_dir = "../data/"

        self.r_file = open("plot.r", "w")

        self.r_file.write("pdf(\"plot_latency_benchmark.pdf\")\n")
        self.title = "Latency benchmark"

        self.x_max = -1
        self.y_max = -1

        self.lines_blocking = []
        self.lines_async = []

        self.colors = ["red", "blue", "green", "orange", "black"]
        self.pchs = ["0", "1", "2", "3", "4"]

        index = 0
        self.color_delay_comb = []

        for fn in os.listdir(self.base_dir):
            if fn.startswith('latencybenchmark_made'):
                parts = fn.split("_")
                type = parts[2]
                calls = parts[3]
                delay = parts[4]

                if type == "blocking":
                    self.color_delay_comb.append((float(delay), self.colors[index]))
                    # Do the call myself since I want to group them on index
                    self.parse_files("latencybenchmark_made_%s_%s_%s" % ("blocking", calls, delay), "latencybenchmark_done_%s_%s_%s" % ("blocking", calls, delay),
                                type, index)
                    self.parse_files("latencybenchmark_made_%s_%s_%s" % ("async", calls, delay), "latencybenchmark_done_%s_%s_%s" % ("async", calls, delay),
                                "async", index)
                index += 1
                index = index % len (self.colors)

        x_lim = [0, self.x_max]
        y_lim = [0, self.y_max]

        # Sort the delays, color tuples
        self.color_delay_comb = sorted(self.color_delay_comb)

        self.r_file.write("plot(1, xlim=%s, ylim=%s, type = \"l\", xlab = \"Call\", ylab = \"Latency (milliseconds)\", main = \"%s\")\n" %
                         (robjects.IntVector(x_lim).r_repr(), robjects.IntVector(y_lim).r_repr(), self.title))

        # Add all the individual lines
        for i in range (len(self.lines_blocking)):
            self.r_file.write(self.lines_blocking[i])
            self.r_file.write(self.lines_async[i])

        # Add a legend
        self.r_file.write("legend(x=0, y=%s, %s, lty=%s, lwd=%s, col=%s, title=\"Delay (s)\")\n" %
                          (int(self.y_max*0.97), robjects.FloatVector([float(i[0]) for i in self.color_delay_comb]).r_repr(), robjects.IntVector([1] * len(self.colors)).r_repr(),
                           robjects.FloatVector([2.5] * len(self.colors)).r_repr(), robjects.StrVector([i[1] for i in self.color_delay_comb]).r_repr()))
        self.r_file.write("dev.off()\n")

        # Flush and write the file
        self.r_file.flush()
        self.r_file.close()

        # create the plot with R
        subprocess.call("R < plot.r --no-save", shell=True)

    def parse_files(self, made_file, done_file, type, index):
        made_file = open(os.path.join(self.base_dir, made_file), "r")
        done_file = open(os.path.join(self.base_dir, done_file), "r")
        blocking_delays = {}
        blocking_start = {}

        for line in made_file.readlines():
            parts = line.split(" ")
            blocking_start[int(parts[0])] = (int(float(parts[1])), int(parts[2]))

        for line in done_file.readlines():
            parts = line.split(" ")
            blocking_delays[int(parts[0])] = (int(float(parts[1])),
                                              int(parts[2]) # logged call time
                                                - int(blocking_start[int(parts[0])][1]) # begin time
                                                - int(blocking_start[int(parts[0])][0]) # set delay in the callLater
                                              )


        x_vals = []
        y_vals = []

        for key in blocking_delays.keys():
            x_vals.append(key)
            y_vals.append(blocking_delays[key][1])
            # print blocking_delays[key][0]

        self.x_max = max(self.x_max, max(x_vals))
        self.y_max = max(self.y_max, max(y_vals))

        if type == "blocking":
            self.lines_blocking.append("lines(%s, %s, type = \"l\", col=\"%s\", pch=%s, lty=1)\n" %
                                       (robjects.IntVector(x_vals).r_repr(), robjects.IntVector(y_vals).r_repr(), self.colors[index], self.pchs[index]))
        else:
            self.lines_async.append("lines(%s, %s, type = \"l\", col=\"%s\", pch=%s, lty=2)\n" %
                                       (robjects.IntVector(x_vals).r_repr(), robjects.IntVector(y_vals).r_repr(), self.colors[index], self.pchs[index]))


if __name__ == "__main__":
    LatencyBenchmarkDataParser()
