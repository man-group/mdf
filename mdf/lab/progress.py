import sys
import uuid
from mdf.remote import messaging

_fmt = "%Y-%m-%d %H:%M:%S"


class ProgressBar(object):
    def __init__(self, start_date, end_date):
        # this is only approximate as it doesn't take weekends into account
        self.start_date = start_date
        self.num_days = (end_date - start_date).days
        self._date = start_date
        self.prog_bar = '[]'
        self.fill_char = '*'
        self.width = 40
        self.set_date(self.start_date)

        # register our callback with the messaging api
        self._subj = uuid.uuid1()
        messaging.register_message_handler(self._subj, self.__msg_callback)

    def __call__(self, date, ctx):
        # send to the parent process (or call directly if this is the parent process)
        messaging.send_message(self._subj, date)

    def __del__(self):
        try:
            # this will fail in the child processes but just ignore it
            messaging.unregister_message_handler(self._subj, self.__msg_callback)
        except RuntimeError:
            pass

    def __msg_callback(self, subject, date):
        if date <= self._date:
            return
        self.set_date(date)
        sys.stdout.write("\r")
        sys.stdout.write(str(self))
        sys.stdout.flush()

    def set_date(self, date):
        done = float((date - self.start_date).days)
        self.__update_amount((done / self.num_days) * 100.0)
        self.prog_bar += \
            ' processed %s - %d of %s days complete' % (date.strftime(_fmt), done, self.num_days)
        self._date = date

    def __update_amount(self, new_amount):
        percent_done = int(round((new_amount / 100.0) * 100.0))
        all_full = self.width - 2
        num_hashes = int(round((percent_done / 100.0) * all_full))
        self.prog_bar = '[' + self.fill_char * num_hashes + ' ' * (all_full - num_hashes) + ']'
        pct_place = (len(self.prog_bar) // 2) - len(str(percent_done))
        pct_string = '%d%%' % percent_done
        self.prog_bar = self.prog_bar[0:pct_place] + \
                        (pct_string + self.prog_bar[pct_place + len(pct_string):])

    def __str__(self):
        return str(self.prog_bar)

    def combine_result(self, other, other_ctx, ctx):
        self.set_date(other_ctx.get_date())
