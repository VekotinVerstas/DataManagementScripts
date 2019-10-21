import argparse
import logging
import sys
import os
import matplotlib
import matplotlib.dates as mdates
import pandas as pd

matplotlib.use('Agg')
import matplotlib.pyplot as plt
from pandas.plotting import register_matplotlib_converters

register_matplotlib_converters()

matplotlib.rcParams['figure.figsize'] = [8.0, 6.0]
matplotlib.rcParams['figure.dpi'] = 80
matplotlib.rcParams['savefig.dpi'] = 300

matplotlib.rcParams['font.size'] = 10
matplotlib.rcParams['legend.fontsize'] = 'x-small'
matplotlib.rcParams['figure.titlesize'] = 'x-small'
matplotlib.rcParams['figure.figsize'] = (11.69, 8.27)

days = mdates.WeekdayLocator()
hours = mdates.HourLocator(interval=1)
time_fmt = mdates.DateFormatter('%Y-%m-%d')

MQTT_CONNECTED = False


def usage():
    print(f"""

Example usage:

python {sys.argv[0]} -f ... TODO TODO
    """)
    exit()


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-l", "--log", dest="log", choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                        default='ERROR', help="Set the logging level")
    parser.add_argument('--dryrun', action='store_true', help='Do not really send, just do everything else')
    parser.add_argument("-f", "--filename", help="CSV filename", required=True)
    parser.add_argument("--outpath", help="Directory where to save all files (default current directory", default='.')
    parser.add_argument("-c", "--columns", help="Comma separated list of data column names to visualise",
                        required=False)
    parser.add_argument("-s", "--separator", help="CSV field separator", default=',', required=False)
    parser.add_argument("-ip", "--hostname", help="Database address (ip/url)", default="localhost", nargs='?')
    parser.add_argument("-d", "--devid", help="Comma separated list of device ids", required=False)
    parser.add_argument("-tl", "--timelength", help="Length of time for dump in minutes",
                        choices=[1, 2, 3, 4, 5, 6, 10, 12, 15, 20, 30, 60], default=5, type=int, nargs='?')
    parser.add_argument("--ylabel", help="Y-axis label", default="")
    parser.add_argument("--ylim", help="Y-axis min and max values", default="0,100")
    parser.add_argument("--usage", action='store_true', help='Print usage text and exit')
    args = parser.parse_args()
    if args.usage:
        usage()
    if args.log:
        logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s',
                            level=getattr(logging, args.log))
    return args


def show_available_measurements(args):
    with open(args.filename, 'rt') as f:
        header_line = f.readline().strip()
    cols = header_line.split(args.separator)
    data_cols = cols[2:-1]
    print(f'Define explicitly data columns to visualise, e.g.')
    print(' --columns {}'.format(','.join(data_cols)))


def show_available_devids(args):
    devids = set()
    with open(args.filename, 'rt') as f:
        f.readline()
        for line in f.readlines():
            devids.add(line.strip().split(args.separator)[-1])
    print(f'Define explicitly device ids to visualise, e.g.')
    print(' --devid {}'.format(','.join(devids)))


def generate_pdf(args):
    fbname = os.path.splitext(os.path.basename(args.filename))[0]
    pdfname = fbname + '.pdf'
    csvname = fbname + '.csv'
    devids = args.devid.split(',')
    cols = args.columns.split(',')
    df = pd.read_csv(args.filename, sep=args.separator)
    colors = ['b', 'r', 'g', 'y', 'c', 'b']
    fig, ax = plt.subplots()
    for devid in devids:
        _df = df.loc[df['dev-id'] == devid]
        _df['readable_time'] = pd.to_datetime(_df['readable_time'])
        _df = _df.set_index('readable_time', drop=False)
        _df = _df.drop(['time'], axis=1)
        _df = _df.resample(rule='5Min', on='readable_time').mean()
        for col in cols:
            label = f'{col} {devid}'
            _df[col] = _df[_df[col] < 2000][col]
            coldata = _df[col]
            ax.plot(_df.index, coldata, color=colors.pop(0), linewidth=0.5, label=label)
        if _df.size > 0:
            full_path = os.path.join(args.outpath, f'{fbname}-{devid.replace(":", "")}.csv')
            export_csv = _df.to_csv(full_path, header=True)
    ax.xaxis.set_major_formatter(time_fmt)
    ax.xaxis.set_minor_locator(hours)
    ax.grid(True)
    fig.autofmt_xdate()
    plt.legend(loc='upper right')
    plt.ylabel(args.ylabel)
    ylim = [int(x) for x in args.ylim.split(',')]
    plt.ylim(ylim)
    full_path = os.path.join(args.outpath, pdfname)
    plt.savefig(full_path)
    # _df = df.loc[df['dev-id'].isin(devids)]
    # export_csv = _df.to_csv(fbname, header=True)


def csv2email():
    args = get_args()
    if args.columns is None:
        show_available_measurements(args)
        exit(1)
    if args.devid is None:
        show_available_devids(args)
        exit(1)
    if args.outpath != '.' and not os.path.isdir(args.outpath):
        os.makedirs(args.outpath)
    generate_pdf(args)


if __name__ == '__main__':
    csv2email()
