#!/usr/bin/python3
from datetime import datetime, timedelta
import argparse
import re
import subprocess
import sys

class ZFSError(IOError):
    pass

class NoMatchingSnapshots(ZFSError):
    pass

# Main class for the Yazbaka (Yet Another ZFS Backup Application)
class Yazbaka:
    VERSION = "0.1"  # Version of the Yazbaka application

    def __init__(self, args):
        """
        Initializes the Yazbaka object. If the arguments have not been validated via the validate method, this
        constructor will attempt to do that and will Raise a value error if that fails.

        Args:
            args: An argparse.Namespace object containing the command-line arguments.
        """
        # Validate arguments if not already validated
        if not hasattr(args, "validated") or not args.validated:
            Yazbaka.validate_args(args, False)
        self.args = args
        self.now = datetime.now()
        # Generate a timestamp in ISO format (YYYY-MM-DD_HHMM)
        self.timestamp = self.now.isoformat(timespec='minutes', sep="_").replace(":", "")
        self.snapshot_name = self.args.label + "_" + self.timestamp
        self.snaps = []

    def has_changed(self):
        """
        Determine if there have been any changes since the last yazbaka snapshot.

        Returns:
            bool: True if a new snapshot is justified.
        """
        cmd = ["zfs", "get", "-Hpr", "written", self.args.source]
        res = subprocess.run(cmd, capture_output=True)
        if res.returncode != 0:
            raise ZFSError(res.stderr.decode())

        zero = True
        true_recursive = 'R' in self.args.send
        nested_dataset_slash = self.args.source + "/"

        for line in res.stdout.splitlines():
            line = line.decode()
            elements  = line.split("\t")

            if line.find(nested_dataset_slash) != -1 and not true_recursive:
                continue

            # If it's not a snapshot, any writing has been done since the last snapshot
            if line.find('@') == -1:
                if int(elements[2]) != 0:
                    return True
                else:
                    zero = True
                    continue

            # Disregard written value for yaz snaps, value is previous to current
            if line.find('@' + self.args.label) != -1 :
                zero = True
            else: #  Not a yazbaka snapshot, and writes count
                if int(elements[2]) != 0:
                    zero = False

        return not zero

    def transfer(self):
        """
        Performs the transfer using ZFS send/recv (incremental or new backup).
        """
        if self.args.incremental or self.args.full_incremental:
            self.incremental_backup()
        else:
            self.new_backup()

    def new_backup(self):
        """
        Performs a new (full) backup.
        """
        try:
            last_snap = self.list_yaz_snapshots(self.args.source)[-1]
            self.snapshot_name = re.search(r"(?<=@).*$", last_snap).group(0)
        except:
            raise NoMatchingSnapshots("There are no snapshots tha can be sent")

        send  = "zfs send " + self.args.send + " " + self.args.source + "@" + self.snapshot_name
        recv = "zfs recv " + self.args.recv + " " + self.args.destination
        cmd = send + " | " + recv

        if self.args.verbose:
            print(cmd)

        res = subprocess.run(cmd, capture_output=True, shell=True)
        if res.returncode != 0:
            raise ZFSError(res.stderr.decode())



    def incremental_backup(self):
        """
        Performs an incremental backup.
        Finds matching snapshots between source and destination to determine the last common snapshot.
        """

        src = self.list_yaz_snapshots(self.args.source)
        dest  = self.list_yaz_snapshots(self.args.destination)
        pairs = Yazbaka.get_pairs(src,dest)  # Get matching snapshot pairs
        if len(pairs) == 0:
            raise NoMatchingSnapshots("There are no matching snapshots between source and destination") # Raise error if no matching snapshots found

        src_start = self.args.source + "@" + pairs[-1]
        src_end = src[-1]
        if src_start == src_end:
            raise NoMatchingSnapshots("There are no snapshots tha can be sent")

        if self.args.full_incremental:
            iflag = " -I "
        else:
            iflag = " -i "

        send  = "zfs send " + self.args.send + iflag + src_start + " " + src_end
        recv = "zfs recv " + self.args.recv + " " + self.args.destination
        cmd = send + " | " + recv

            if self.args.verbose:
            print(cmd)

        res = subprocess.run(cmd, capture_output=True, shell=True)
        if res.returncode != 0:
            raise ZFSError(res.stderr.decode())


    @staticmethod
    def get_pairs(src, dest):
        """
        Compares two lists of snapshot names to find common snapshots based on their timestamp.

        Args:
            src (list): A list of source snapshot names.
            dest (list): A list of destination snapshot names.

        Returns:
            list: A list of matching snapshots names (everything after the @).
        """
        i = j = 0
        pairs = []
        while i < len(src) and j < len(dest):
            cur_src = re.search(r"@.*$", src[i]).group(0)[1:]
            cur_dest = re.search(r"@.*$", dest[j]).group(0)[1:]

            if cur_src == cur_dest:
                pairs.append(cur_src)
                i += 1
                j += 1
                continue

            src_stamp = Yazbaka._get_timestamp(cur_src)
            dest_stamp = Yazbaka._get_timestamp(cur_dest)

            # Skip snapshots that don't contain a timestamp
            if src_stamp is None:
                i += 1
                continue
            if dest_stamp is None:
                j += 1
                continue

            # Advance the pointer of the list with the older timestamp
            if src_stamp < dest_stamp:
                i += 1
            else:
                j += 1

        return pairs

    @staticmethod
    def _get_timestamp(string):
        """
        Extracts a timestamp string from a given string.

        Args:
            string (str): The string to extract the timestamp from.

        Returns:
            str or None: The extracted timestamp string if found, otherwise None.
        """
        match = re.search(r"[0-9][0-9\-_T]*$", string)
        if match is None:
            return None
        return match.group(0)

    def conditional_snapshot(self):
        """
        Creates a new ZFS snapshot if the appropriate conditions are met per the command line args.

        Returns:
           True: on a successful snapshot
           False: if the snapshot is not performed

        """
        if self.args.transfer_only:
            if not self.args.quiet:
                print("Transfer only: omitting snapshot")
            return False

        if (not self.has_changed()) and not self.args.no_omit_unchanged:
            if not self.args.quiet:
                print("No change: omitting snapshot")
            return False

        if self.args.omit:
            self.snaps = self.list_yaz_snapshots(self.args.source)
            if self.snaps:
                last_snap = self.snaps[-1]
                timestamp = re.search(r"[0-9][0-9\-_]*$", last_snap).group(0)
                timestamp = timestamp[:-2] + ":" + timestamp[-2:]  # Format timestamp for datetime parsing

                # If the last snapshot was taken within the 'omit' duration, skip
                if self.args.omit > self.now - datetime.fromisoformat(timestamp):
                    if not self.args.quiet:
                        print("Too recent: omitting snapshot")
                    return False

        return self.snapshot()

    def snapshot(self):
        """
        Creates a new ZFS snapshot of the source dataset.

        Returns:
           True: on a successful snapshot

        Raises:
            PermissionError: If the user does not have appropriate permissions
            ZFSError: If there are any other errors with from the ZFS command
        """
        # Take the snapshot
        if "R" in self.args.send:  # Check if recursive snapshot is requested (from zfs send flags)
            cmd = ["zfs", "snapshot", "-r", self.args.source + "@" + self.snapshot_name]
        else:
            cmd = ["zfs", "snapshot", self.args.source + "@" + self.snapshot_name]

        if not self.args.quiet:
            print("Snapping...")
        if self.args.verbose:
            print(cmd)
        res = subprocess.run(cmd, capture_output=True)

        if res.returncode != 0:
            if res.stderr.decode().find("permission denied"):
                raise PermissionError(res.stderr.decode())
            raise ZFSError(res.stderr.decode())

        return True  # Indicate successful snapshot creation

    def cleanup(self):
        """
        Handles the cleanup of old snapshots.
        (Currently a placeholder, actual implementation would involve deleting old snapshots).
        """
        pass  # Placeholder for cleanup implementation

    def do_all(self):
        """
        Executes the main backup workflow: snapshot, transfer, and cleanup.
        """
        self.conditional_snapshot()

        if self.args.snap_only:
            return

        if not self.args.quiet:
            print("Transferring...")
        self.transfer()

        if not self.args.no_cleanup:
            if not self.args.quiet:
                print("Cleaning up")
            self.cleanup()

    @staticmethod
    def _get_timedelta(time_str):
        """
        Converts a time string (e.g., "3h", "5days") into a timedelta object.

        Args:
            time_str (str): The time string to convert.

        Returns:
            datetime.timedelta: A timedelta object representing the duration.

        Raises:
            ValueError: If the time string format is invalid.
        """
        match = re.match(r"^[0-9]*", time_str)  # Extract the numeric part
        if match:
            num_str = match.group(0)
            num = int(num_str)
            spec_str = time_str[len(num_str):]  # Extract the unit part

            # Convert to timedelta based on the unit
            if  spec_str in ['m', 'minute', 'minutes']:
                return timedelta(minutes=num)
            elif spec_str in ['h', 'hour', 'hours']:
                return timedelta(hours=num)
            elif spec_str in ['d', 'day', 'days']:
                return timedelta(days=num)
            elif spec_str in ['w', 'week', 'weeks']:
                return timedelta(weeks=num)
            elif spec_str in ['mon', 'month', 'months']:
                return timedelta(days=num * 31)  # Approximation for months
            elif spec_str in ['y', 'year', 'years']:
                return timedelta(days=num * 365)  # Approximation for years
            else:
                raise ValueError("Invalid time format. Use -h for help.")
        else:
            raise ValueError("Invalid time format. Use -h for help.")

    def list_yaz_snapshots(self, dataset):
        """
        Lists snapshots created by Yazbaka for a specific dataset.

        Args:
            dataset (str): The ZFS dataset to list snapshots for.

        Returns:
            list: A list of Yazbaka-created snapshot names.

        Raises:
            ZFSError: If the zfs list command fails.
        """
        cmd = ["zfs", "list", "-H", "-t", "snapshot", dataset]
        res = subprocess.run(cmd, capture_output=True)
        if res.returncode != 0:
            raise ZFSError(res.stderr.decode())

        snapshots = []
        for line in res.stdout.splitlines():
            line = line.decode()
            # Filter for snapshots with the Yazbaka label
            if line.find(dataset + "@" + self.args.label) != -1:
                snapshots.append(re.match(r"^[^\t]*", line).group(0))  # Extract full snapshot name

        return snapshots

    @staticmethod
    def list_all_snapshots(dataset=None):
        """
        Lists all ZFS snapshots for a given dataset, or all snapshots if no dataset is specified.

        Args:
            dataset (str, optional): The ZFS dataset to list snapshots for. Defaults to None (all snapshots).

        Returns:
            list: A list of all snapshot names.
        """
        cmd = ["zfs", "list", "-H", "-t", "snapshot"]
        if dataset:
            cmd.append(dataset)

        res = subprocess.run(cmd, capture_output=True)
        snapshots = []
        for line in res.stdout.splitlines():
            line = line.decode()
            if dataset is None:
                snapshots.append(re.match(r"^[^\t]*", line).group(0))
            elif line.find(dataset + "@") != -1:
                snapshots.append(re.match(r"^[^\t]*", line).group(0))

        return snapshots

    @staticmethod
    def validate_args(args, exit_on_error=True):
        """
        Validates and processes the command-line arguments provided to Yazbaka.

        Args:
            args: An argparse.Namespace object containing the command-line arguments.
            exit_on_error (bool): If True, the program will exit on validation error; otherwise, it raises a ValueError.

        Raises:
            ValueError: If validation fails and exit_on_error is False.
        """
        # Check for conflicting incremental arguments
        if args.full_incremental and  args.incremental:
            Yazbaka._exit_throw(exit_on_error,
                                "Conflicting arguments: full_incremental and incremental, use -h for help",
                                "Conflicting arguments: full_incremental and incremental")

        # Validate zfs send flags
        args.send = args.send.replace("'", "")
        for flag in args.send:
            if flag not in 'DLPRbcehnpsvw':
                Yazbaka._exit_throw(exit_on_error,
                    "Invalid send flag: %s! Type -h for options or consult the zfs-send man page." % flag,
                    "Invalid send flag: %s" % flag)
        if args.send:
            args.send = "-" + args.send

        # Validate zfs receive flags
        args.recv = args.recv.replace("'", "")
        for flag in args.recv:
            if flag not in 'FhMnsuv':
                Yazbaka._exit_throw(exit_on_error,
                    "Invalid receive flag: %s! Type -h for options or consult the zfs-recv man page." % flag,
                    "Invalid receive flag: %s" % flag)
        if args.recv:
            args.recv = "-" + args.recv

        # Validate source dataset format
        args.source = source = args.source.replace("'", "")
        if not re.match(r"[^/]+/[^/]+.*", source):
            msg = "Source: %s does  not appear valid" % source
            Yazbaka._exit_throw(exit_on_error, msg, msg)

        # Validate destination dataset format (if provided)
        if hasattr(args, 'destination'):
            args.destination = destination = args.destination.replace("'", "")
            if not re.match("[^/]+/[^/]+.*", destination):
                msg = "Destination: %s does  not appear valid" % destination
                Yazbaka._exit_throw(exit_on_error, msg, msg)
        else:
            destination = "-" # Default for snap-only or when destination is not required

        # Check if source or destination starts with '/'
        if source[0] == "/" or destination[0] == "/":
            msg = "Source and destination cannot start with '/'"
            Yazbaka._exit_throw(exit_on_error, msg , msg)

        # Convert omit time string to timedelta object
        if args.omit:
            args.omit = Yazbaka._get_timedelta(args.omit.replace("'", ""))

        # Clean up label string
        if args.label:
            args.label = args.label.replace("'", "")

        args.validated = True

        if args.verbose:
            print(args)

    @staticmethod
    def _exit_throw(exit1, exit_msg, exception_msg):
        """
        Helper method to either exit the program or raise a ValueError based on a flag.

        Args:
            exit1 (bool): If True, exit the program. Otherwise, raise ValueError.
            exit_msg (str): Message to print before exiting.
            exception_msg (str): Message for the ValueError if raised.
        """
        if exit1:
            print(exit_msg)
            exit(1)
        else:
            raise ValueError(exception_msg)

    @staticmethod
    def parse_args():
        """
        Parses command-line arguments using argparse.

        Returns:
            argparse.Namespace: An object containing the parsed arguments.
        """
        desc = """Yazbaka: Yet Another Zfs BAcKup Application.
        This was primarily built to support backing up to offline media that is removed from the system when not in use and cannot be performed per a fixed schedule.
        It creates a new snapshot at execution time so it is never sending a stale one, and it maintains a configurable number or duration so they don't accrue indefinitely.
        It uses zfs send/recv to transfer files to the destination pool.  Using SSH to send to a remote pool is not supported at this time, but is planned.
        Normally no snapshot will be made if no data has been written since the last yazbaka snapshot and nothing will be transferred 
        """
        epilog = """
        For time polices used with the -o option combine a number with a unit not using any spaces. Single digit units can be used with
        the exception of months. (e.g. 3h or 3hours). Acceptable units are m[minutes], h[ours], d[ays], w[eeks],
        mon[ths], y[ears]. Multiple units cannot be combined. Months are assumed to be 31 days.
        """
        parser = argparse.ArgumentParser(prog='yazbaka.py', description=desc, epilog=epilog)

        parser.add_argument('-i', '--incremental', help="Incremental (same as using i with zfs send)", action="store_const", const=True)
        parser.add_argument('-I', '--full-incremental', help="Full incremental, send all intermittent snapshots (same as using I with zfs send)", action="store_const", const=True)
        parser.add_argument('-n', '--no-cleanup', help="Do not remove any snapshots from the source.", action="store_const", const=True)
        parser.add_argument('-d', '--delete', help="Delete old snapshots on the destination following the same retention policy", action="store_const", const=True)
        parser.add_argument('-o', '--omit',  help="Skip the snapshot if the most recent snapshot is less than specified time ago",  type=ascii)
        # parser.add_argument('-p', '--progress', help="Show progress", action="store_const", const=True) # Commented out progress argument
        parser.add_argument('-k', help="Set the number or duration of matched snapshots to keep on the source (default=5).", type=int, default=5)
        parser.add_argument('-l', '--label', help="Label used for the snapshot. (default yazbak)", type=ascii, default="yazbak")
        parser.add_argument('-s', '--send', help=" [DLPRbcehnpsvw] flags to pass to zfs send (see man zfs-send for flag descriptions) ", type=ascii, default="")
        parser.add_argument('-r', '--recv', '--receive', help="[FhMnsuv] flags to pass to zfs recv (see man zfs-recv for flag descriptions)",  type=ascii, default="")
        parser.add_argument('--snap-only', help="Only take a snapshot, do NOT transfer. Destination should be ommited if this argument is used. (Useful if you want to stop a service before making the snapshot and restart it before send)", action="store_const", const=True)
        parser.add_argument('--transfer-only', help="Only send, do NOT take a snapshot", action="store_const", const=True)
        parser.add_argument('--no-omit-unchanged', help="Still create a snapshot even if no data has been written since the last one.", action="store_const", const=True) # Not implemented
        #parser.add_argument('--no-omit-exists', help="Still attempt to transfer even if the latest snapshot exists on the destination pool. This will force an 'f' in send options!", action="store_const", const=True) # Not implemented, add the code to force the f
        parser.add_argument('-q', '--quiet', help="", action="store_const", const=True)
        parser.add_argument('-v', '--verbose', help="", action="store_const", const=True)

        parser.add_argument('source', help="source dataset (e.g. mypool/dataset)", type=ascii)
        # Destination argument is conditional; not required if --snap-only is used
        if "--snap-only" not in sys.argv:
            parser.add_argument('destination', help="source dataset (e.g. mypool/dataset)", type=ascii)
        parser.add_argument('--version', action='version', version='%(prog)s: ' + Yazbaka.VERSION)
        return parser.parse_args()


if __name__ == "__main__":
    # Parse command-line arguments
    args = Yazbaka.parse_args()
    # Validate the parsed arguments
    Yazbaka.validate_args(args)
    # print(args) # Debugging line to print parsed arguments
    # Create an instance of the Yazbaka class
    backup = Yazbaka(args)
    # Execute the main backup workflow
    backup.do_all()
