import json
import socket
import time
import urllib2
import utils.mail

import metrics_pusher_client
import owl_config

from django.core.management.base import BaseCommand, CommandError

from metrics_pusher_client import FalconCli
from optparse import make_option

from owl_config import ALERT_METRICS_ADDR
from owl_config import ALERT_STEP
from owl_config import METRICS_PUSH_ALERT_ADMINS

SUB_METRICS_SIZE = 80000
PUSH_SUCCESS = 'OK'

class MetricsPusher:
  def __init__(self, options):
    self.mailer = utils.mail.Mailer(options)
    self.metrics_data = None
    self.push_failed_count = 0
    self.push_failed_msg = []

  def push(self):
    # URLError/HTTPError exception may be thrown
    # Large data, easy to timeout, set 5mins
    self.metrics_data = json.load(
      urllib2.urlopen(ALERT_METRICS_ADDR, None, 300))

    if not self.metrics_data:
      print "No metrics data for uri: %s" % ALERT_METRICS_ADDR
      # send email
      self.mailer.send_email(subject = 'No metrics error',
        content = "No metrics data for uri: %s" %(ALERT_METRICS_ADDR),
        to_email = METRICS_PUSH_ALERT_ADMINS,
      )
      return None

    # using the interface of alert system to push metrics
    sub_metrics_group = len(self.metrics_data) / SUB_METRICS_SIZE + 1
    for i in range(sub_metrics_group):
      sub_metrics = self.metrics_data[i*SUB_METRICS_SIZE:(i+1)*SUB_METRICS_SIZE]
      if not sub_metrics:
        continue

      replay_counter = 0
      while replay_counter < 3:
        try:
          cli = FalconCli.connect(debug=False, buf_size=8000)
          if not cli:
            raise socket.error('falcon connection failure.')
          response = cli.update(sub_metrics)
        except Exception as e:
          replay_counter += 1
          self.mailer.send_email(subject = 'Metrics pushing error',
            content = "A bad response from alert system server: %r\n" \
              "The pusher script will replay it a maximum of 3 times.\n" \
              "Id: %d replay times" % (repr(e), i),
            to_email = METRICS_PUSH_ALERT_ADMINS,)
          time.sleep(5)
        else:
          break

      time.sleep(1)

    # analyze the response of pushing operation
    if not response:
      self.push_failed_count += 1
      self.push_failed_msg.append("Response for pushing is None.")

    for item in response:
      if item.get('Msg').lower() != 'ok':
        self.push_failed_count += 1
        self.push_failed_msg.append("Response for pushing is not ok.")
      if item.get('ErrInvalid') > 0:
        self.push_failed_count += 1
        self.push_failed_msg.append("Response for pushing has invalid item.")

    if self.push_failed_count > 3:
      # send email
      response_exception = Exception("Response error: %s" % str(
        self.push_failed_msg))
      self.push_failed_count = 0
      self.push_failed_msg = []
      raise response_exception

    return PUSH_SUCCESS

class Command(BaseCommand):
  args = ''
  help = "Run the background metrics push script to push metrics to alert system."

  option_list = BaseCommand.option_list + (
    make_option(
      "--period",
      default = ALERT_STEP, # push per 5 minute
      help = "Check the period of pushing alert metrics."
    ),
  )

  def handle(self, *args, **options):
    self.args = args
    self.options = options
    self.mailer = utils.mail.Mailer(options)

    self.stdout.write("args: %r\n" % (args, ))
    self.stdout.write("options: %r\n" % options)

    metrics_pusher = MetricsPusher(self.options)
    start_time, end_time = 0, 0
    while True:
      try:
        start_time = time.time()
        push_status = metrics_pusher.push()
        end_time = time.time()
      except Exception as e:
        # send alert email when pushing program appears error
        self.mailer.send_email(subject = 'Metrics pusher error',
          content = repr(e),
          to_email = METRICS_PUSH_ALERT_ADMINS,
        )

      sleep_time = int(self.options['period']) - int(end_time - start_time)
      if sleep_time > 0:
        time.sleep(sleep_time)

