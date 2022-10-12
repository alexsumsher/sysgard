# sysgard
a simple schedule/event handler


from multiprocessing import Queue
from background_jobs import act_maintain

mail_queue = Queue(64)

def mjob1_test_mail(Q=None):
  pass

maintain_jobs = [
  event.new_event(event.ETYPE_MINUTES, 'jobtest', mjob1_test_mail, 5, runmode=event.RUN_FUNC),
  #event.new_event(event.ETYPE_MINUTES, 'test_offline', act_maintain.alert_offline, 30, runmode=event.RUN_THREAD),
  event.new_event(event.ETYPE_HOURS, 'do_some_alerts', act_maintain.alert_reporter, 0.5, runmode=event.RUN_THREAD),
  event.new_event(event.ETYPE_DAILY, 'scan_for_alert', act_maintain.alert_deep_scan, "00:50:00", runmode=event.RUN_THREAD),
  event.new_event(event.ETYPE_DAILY_TIMES, 'get_weathers', act_maintain.weather_collect, "2:00:00,4:00:00,6:00:00,8:00:00,10:00:00,12:00:00,14:00:00,16:00:00,18:00:00,20:00:00,22:00:00", runmode=event.RUN_THREAD),
  event.new_event(event.ETYPE_MONTHLY, 'log_store', act_maintain.monlog_store, 6, '10:00:00', runmode=event.RUN_THREAD),
  event.new_event(event.ETYPE_YEARLY, 'yearly_table', act_maintain.yearly_table, "12-31", "23:00:00", runmode=event.RUN_THREAD),
]
maintainer = sys_gardD()
maintainer.add_events(*maintain_jobs)
maintainer.bind_arg('do_alerts', mail_queue)
maintainer.start()
