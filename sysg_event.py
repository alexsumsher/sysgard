import datetime
import time
import logging

"""
UPDATE LOGS:
2020-12-18: +FREE_EVENT
"""

DT = datetime.datetime

class event(object):

	ETYPE_ONETIME = 1 #datetime
	ETYPE_DAILY = 2 #time
	ETYPE_MONTHLY = 3 #day,time
	ETYPE_YEARLY = 4 #
	ETYPE_HOURS = 10
	ETYPE_MINUTES = 11
	# subclass other
	ETYPE_DAILY_TIMES = 21
	ETYPE_FREE = 99

	RUN_FUNC = 0
	RUN_THREAD = 1

	day_seconds = 24 * 60 * 60
	h_offset = 8
	tstamp_offset = h_offset * 60 * 60
	# tstamp_offset = time.timezone
	# origin_time - tstamp_offset => real time stamp (by minus)
	# on event object use: origin_time + tstamp_offset(by plus)

	event_timeout = 5
	time_slot = 20
	# 启动的当日00:00:00为时间起点
	# timestamp 其实为8小时 = 
	# Works with timestamp
	
	@staticmethod
	def new_event(etype, name, act, *dtargs, runmode=0):
		# runmode:
		# 0: function
		# 1: thread
		if etype == event.ETYPE_ONETIME:
			return evt_onetime(name, act, *dtargs, runmode=runmode)
		elif etype == event.ETYPE_DAILY:
			return evt_daily(name, act, *dtargs, runmode=runmode)
		elif etype == event.ETYPE_MONTHLY:
			return evt_monthly(name, act, *dtargs, runmode=runmode)
		elif etype == event.ETYPE_YEARLY:
			return evt_yearly(name, act, *dtargs, runmode=runmode)
		elif etype == event.ETYPE_HOURS:
			return evt_hours(name, act, *dtargs, runmode=runmode)
		elif etype == event.ETYPE_DAILY_TIMES:
			return evt_daily_times(name, act, *dtargs, runmode=runmode)
		elif etype == event.ETYPE_MINUTES:
			return evt_minutes(name, act, *dtargs, runmode=runmode)
		elif etype == event.ETYPE_FREE:
			raise ValueError("FREE EVENT should not create by this way!")
		else:
			raise ValueError("Unkown Event type!")

	def __init__(self, name, act, *dtargs, runmode=0):
		self.name = name
		# ondate(DT.date)/onhour(int:0-23)/onminute(int:0-59)
		# seconds: on act second
		# until_next_secs: from base_stamp
		self.act = act
		self.runmode = runmode
		self.arg = None
		self.ecount = 0
		self.new_dtime(*dtargs)
		self._result = None

	def _time_str2sec(self, tstr):
		"""
		time_str: "00:00:00"(HH:MM:SS) or "00:00"(HH:MM)
		"""
		_tlist = [int(_) for _ in tstr.split(":")]
		if len(_tlist) == 3:
			return _tlist[0]*3600 + _tlist[1]*60 + _tlist[2]
		elif len(_tlist) == 2:
			return _tlist[0]*3600 + _tlist[1]*60
		else:
			raise ValueError("Not correct time string.")

	def bind_arg(self, arg):
		logging.info("an argument %s bind to event." % str(arg))
		self.arg = arg

	def reduce(self, t_second):
		self.until_next_secs -= t_second

	def cal_next(self, check_dt, check_tstamp=0, force_next=False):
		# calculate for the number of seconds until next time event to eval.
		raise NotImplementedError

	def new_dtime(self, *dtargs):
		raise NotImplementedError

	def __repr__(self):
		return self.name


class evt_onetime(event):
	
	def __init__(self, name, act, ondtime, runmode=0):
		self.type = self.__class__.ETYPE_ONETIME
		super(evt_onetime, self).__init__(name, act, ondtime, runmode=runmode)

	def cal_next(self, check_dt, check_tstamp=0, force_next=False):
		# if self.until_next_secs < 0, should have been remove[no more exists in queue]
		check_tstamp = check_tstamp or check_dt.timestamp()
		check_tstamp = check_tstamp + self.tstamp_offset
		self.until_next_secs = self.on_seconds - check_tstamp
		return self.until_next_secs

	def new_dtime(self, ondtime):
		_ondtime = DT.strptime(ondtime, '%Y-%m-%d %H:%M:%S') if isinstance(ondtime, str) else ondtime
		# self.on_seconds: 执行时间和基础时间（base_tstamp）总差
		self.on_seconds = int(_ondtime.timestamp() + self.tstamp_offset)
		return self.on_seconds


class evt_minutes(event):
	# 间隔N分钟：0<N<60
	# 
	def __init__(self, name, act, in_minutes, runmode):
		if not isinstance(in_minutes, int) or in_minutes<0 or in_minutes>60:
			raise ValueError("minutes should int and 0<minutes<60")
		super(evt_minutes, self).__init__(name, act, in_minutes, runmode=runmode)
		self.type = self.__class__.ETYPE_MINUTES
		self.offset = in_minutes * 60

	def new_dtime(self, in_minutes):
		#print(in_minutes)
		self.on_seconds = int(time.time()) + in_minutes * 60
		return self.on_seconds

	def cal_next(self, check_dt, check_tstamp=0, force_next=False):
		check_tstamp = check_tstamp or check_dt.timestamp()
		_until_next_secs = self.on_seconds - check_tstamp
		# debug
		if _until_next_secs < 0:
			logging.info("minus second: %d" % _until_next_secs)
		if force_next is False:
			check_min = -self.time_slot
		else:
			# 相当于<=0
			check_min = 1
		while _until_next_secs < check_min:
			self.on_seconds += self.offset
			_until_next_secs = self.on_seconds - check_tstamp
		self.until_next_secs = _until_next_secs
		return self.until_next_secs


class evt_hours(event):

	# 间隔N小时0<N<24;从下一个整时间开始算；允许小数点
	def __init__(self, name, act, in_hours, runmode):
		assert 0<in_hours<24
		super(evt_hours, self).__init__(name, act, in_hours, runmode=runmode)
		self.type = self.__class__.ETYPE_HOURS
		self.size = in_hours * 60 * 60
	
	def new_dtime(self, in_hours):
		# self.on_seconds: 下次执行的绝对timestamp
		self.mark_start = int(time.time())
		self.on_seconds = self.mark_start + in_hours * 60 * 60
		return self.on_seconds

	def cal_next(self, check_dt, check_tstamp=0, force_next=False):
		# 负数问题：当_until_next_secs<=0时，只有force_next才有必要强制跳下一次，否则应当保持负数
		# 理论上应该负值在[-timeslot,0]的范围内
		check_tstamp = check_tstamp or int(check_dt.timestamp())
		_until_next_secs = self.on_seconds - check_tstamp
		# debug
		if _until_next_secs < 0:
			logging.info("minus second: %d" % _until_next_secs)
		if force_next is False:
			check_min = -self.time_slot
		else:
			# 相当于<=0
			check_min = 1
		while _until_next_secs < check_min:
			self.on_seconds += self.size
			_until_next_secs = self.on_seconds - check_tstamp
		self.until_next_secs = _until_next_secs
		return self.until_next_secs


class evt_daily_times(event):

	# several times of hours
	def __init__(self, name, act, on_hours, runmode):
		#@on_hours: "12:00:00, 13:00:00, 14:00:00"
		super(evt_daily_times, self).__init__(name, act, on_hours, runmode=runmode)
		self.type = self.__class__.ETYPE_DAILY_TIMES
		self.size = 0

	def _hour_txt2seconds(self, hour_txt):
		# 12:00:00 -> timestamp
		_times = [int(_) for _ in hour_txt.split(":")]
		return _times[0] * 3600 + _times[1] * 60 + (0 if len(_times) == 2 else _times[2])
	
	def new_dtime(self, on_hours):
		# self.on_seconds: 下次执行的绝对timestamp
		if isinstance(on_hours, str):
			hour_list = [_.strip() for _ in on_hours.split(",")]
			seconds_list = [self._hour_txt2seconds(_) for _ in hour_list]
		elif isinstance(on_hours, (list, tuple)):
			seconds_list = []
			for _ in on_hours:
				if isinstance(_, str):
					seconds_list.append(self._hour_txt2seconds(_))
				elif isinstance(_, datetime.datetime):
					seconds_list.append(_.hour * 3600 + _.minute * 60 + _.second)
		# not allow to change
		self.seconds_list = tuple(sorted(seconds_list))
		# test for the recently next: caurse we may start by random time.
		# ct: current time stamp on day offset, ct_of_day_start = 0
		ct = (int(time.time()) % self.day_seconds + self.tstamp_offset)%self.day_seconds
		_pos = 0
		for s in self.seconds_list:
			if s>=ct:
				break
			_pos += 1
		if _pos == len(self.seconds_list):
			# all pass and wait for next time on next day
			_pos = 0
			self.on_seconds = self.day_seconds - ct + self.seconds_list[_pos]
		else:
			self.on_seconds = self.seconds_list[_pos]
		self.pos = _pos
		#print("on newdt", self.pos, ct, "->", self.on_seconds, self.seconds_list)
		return self.on_seconds

	def cal_next(self, check_dt, check_tstamp=0, force_next=False):
		#print("on calnext start", self.pos, self.on_seconds, self.seconds_list)
		# (int(DT.now().replace(hour=2).timestamp())%86400)/3600+8 = 26...
		# so we need a % day-seconds
		check_tstamp = (int(check_tstamp or check_dt.timestamp()) % self.day_seconds + self.tstamp_offset)%self.day_seconds
		# 若是已经过去，则返回下一日的delay
		# force next: starts with next event, caurse current event is eval, 标记是5秒内视为当前事件结束
		# for fater calnext most time, quick handle without a force_next
		delta_max = 1
		if not force_next:
			delta_max = -self.time_slot
			delta = self.on_seconds - check_tstamp
			if delta > delta_max:
				self.until_next_secs = delta
				#print("on calnext end", self.pos, self.on_seconds, self.until_next_secs, self.seconds_list)
				return self.until_next_secs
		_pos = self.pos+1
		xlen = len(self.seconds_list)
		for i in range(_pos, xlen):
			delta = self.seconds_list[_pos] - check_tstamp
			#print(f"delta pos:{_pos} on index {i} is {delta}")
			if delta>delta_max:
				break
			_pos += 1
		if _pos == xlen:
			self.pos = 0
			self.until_next_secs = self.day_seconds - check_tstamp + self.seconds_list[0]
		else:
			self.pos = _pos
			self.until_next_secs = delta
		self.on_seconds = self.seconds_list[self.pos]
		#print("on calnext end", self.pos, self.on_seconds, self.until_next_secs, self.seconds_list)
		return self.until_next_secs

class evt_daily(event):
	
	def __init__(self, name, act, ontime, runmode=0):
		super(evt_daily, self).__init__(name, act, ontime, runmode=runmode)
		self.type = self.__class__.ETYPE_DAILY
		self.run_day = 0
		
	def new_dtime(self, ontime):
		# ontime: '12:12:12' or '12:12' or datetime.time()
		# self.on_seconds: 每日执行相对于00:00:00时的时间偏移量【即每日多少秒执行】
		if isinstance(ontime, str):
			_times = [int(_) for _ in ontime.split(":")]
			self.on_seconds = _times[0] * 3600 + _times[1] * 60 + (0 if len(_times) == 2 else _times[2])
		elif isinstance(ontime, (datetime.time, datetime.datetime)):
			self.on_seconds = ontime.hour * 3600 + ontime.minute * 60 + ontime.second
		else:
			raise ValueError("unknown ontime!")
		return self.on_seconds

	def cal_next(self, check_dt, check_tstamp=0, force_next=False):
		check_tstamp = check_tstamp or int(check_dt.timestamp())
		check_tstamp = check_tstamp + self.tstamp_offset
		# 若是已经过去，则返回下一日的delay
		if force_next is False:
			delta = self.on_seconds - check_tstamp % self.day_seconds
			if delta >= -self.time_slot:
				self.until_next_secs = delta
				return self.until_next_secs
		# should we mark?
		self.until_next_secs = self.on_seconds + (self.day_seconds - check_tstamp % self.day_seconds)
		return self.until_next_secs


class evt_monthly(event):
	MDAYS = (None, 31, None, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31)
	
	def __init__(self, name, act, onday, ontime, runmode=0):
		super(evt_monthly, self).__init__(name, act, onday, ontime, runmode=runmode)
		self.type = self.__class__.ETYPE_MONTHLY

	def _month_days(self, m, y):
		if m == 2:
			return 29 if y%400==0 or (y%4==0 and y%100!=0) else 28
		return self.__class__.MDAYS[m]

	def new_dtime(self, onday, ontime):
		onday = int(onday)
		today = DT.today()
		if today.month == 2 or onday == -1:
			#self._onday = self._month_days(today.month, today.year)
			logging.info("a special month-day event created.")
		elif onday<0 or onday > 31:
			raise ValueError("monthly day over limit.")
		self.onday = onday
		# time, string/datetime.time
		if isinstance(ontime, str):
			_times = [int(_) for _ in ontime.split(":")]
			self.on_seconds = _times[0] * 3600 + _times[1] * 60 + (0 if len(_times) == 2 else _times[2])
		elif isinstance(ontime, (datetime.time, datetime.datetime)):
			self.on_seconds = ontime.hour * 3600 + ontime.minute * 60 + ontime.second
		else:
			raise ValueError("unknown ontime!")

	def cal_next(self, check_dt, check_tstamp=0, force_next=False):
		today = DT.today()
		cur_mdays = self._month_days(today.month, today.year)
		if self.onday == -1:
			thismonth_onday = today.replace(day=cur_mdays, hour=0, minute=0, second=0, microsecond=0)
		else:
			thismonth_onday = today.replace(day=self.onday, hour=0, minute=0, second=0, microsecond=0)
		if force_next is False:
			delta = (thismonth_onday - check_dt).total_seconds() + self.on_seconds
			# before or after
			if delta >= -self.time_slot:
				self.until_next_secs = delta
				return delta
		# next month
		_year = _nyear = check_dt.year
		_month = check_dt.month
		if _month == 12:
			_nmonth = 1
			_nyear += 1
		else:
			_nmonth = _month + 1
		next_mdays = self._month_days(_nmonth, _nyear)
		if self.onday == -1:
			days = cur_mdays - thismonth_onday.day + next_mdays
		elif self.onday > next_mdays:
			# 如果设置为30日，当碰到28/29日时，只能跳过那个月份:
			# 实质上12和1月都是31天，跨年不会出现该情况，故无需在此状况下考虑跨年问题
			_nmonth += 1
			next_mdays = self._month_days(_nmonth, _nyear)
		else:
			days = cur_mdays - thismonth_onday.day + self.onday
		delta = days * self.day_seconds + self.on_seconds
		self.until_next_secs = delta
		return self.until_next_secs


class evt_yearly(event):

	def __init__(self, name, act, ondate, ontime, runmode=0):
		super(evt_yearly, self).__init__(name, act, ondate, ontime, runmode=runmode)
		self.type = self.__class__.ETYPE_YEARLY

	def new_dtime(self, ondate, ontime):
		# ondate: '12-20' in string, or datetime.date[month+day]
		if isinstance(ondate, DT):
			self.month = ondate.month
			self.day = ondate.day
			if ontime is None:
				self.on_seconds = (ondate.hour * 60 + ondate.minute) * 60
		elif isinstance(ondate, str):
			# '12-12', or '12-12 12:12:00'
			self.month = int(ondate[:2])
			self.day = int(ondate[3:5])
			if len(ondate) >= 14:
				time_values = [int(_) for _ in ondate[6:].split(':')]
				self.on_seconds = (time_values[0] * 60 + time_values[1]) * 60
		else:
			raise ValueError("unknown input!")
		if ontime:
			time_values = [int(_) for _ in ontime.split(':')]
			self.on_seconds = (time_values[0] * 60 + time_values[1]) * 60

	def cal_next(self, check_dt, check_tstamp=0, force_next=False):
		ondate = check_dt.replace(month=self.month, day=self.day, hour=0, minute=0, second=0, microsecond=0)
		if force_next is False:
			delta = (ondate - check_dt).total_seconds() + self.on_seconds
			if delta >= -self.time_slot:
				self.until_next_secs = delta
				return delta
		# next year, if onday is leap year february.29, will continue gose to 4years later
		if self.month == 2 and self.day == 29:
			next_date = ondate.replace(year=ondate.year+4)
		else:
			next_date = ondate.replace(year=ondate.year+1)
		delta = (next_date - check_dt).total_seconds() + self.on_seconds
		self.until_next_secs = delta
		return self.until_next_secs


