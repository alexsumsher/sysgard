from threading import Timer, Thread, Lock
import datetime
import time
import logging
from .sysg_event import event


DT = datetime.datetime

class sys_gardD(object):
	"""
	1. event队列初始化，按照延迟时间排列，先后排序
	2. 新增event计算后，按照延迟时间insert到正确位置
	3. 根据第一位置计算sleep（如果sleep大于一日second，则按照一日second延迟），第一位置延迟小于600s则直接按照对应值sleep，否则提前60s；
		提前60s，重新计算首位的延迟时间，以求一定的精准；
	4. wake up后对首event计算，确认时间到，提取，新线程执行，同时重新计算（按照首event的seconds直接全减，实现快速重算）队列所有delay，所
		执行的event根据类型判断移除还是计算下一次的delay，并根据delay结果放到队列中
	5> fix：同时问题，pre_e0 == _e0，当pre_e0运行后重新计算会导致_e0跳过当前序列到下一次序列
	"""
	EMPTY_WAIT = 3600

	def __init__(self, timeslot=20):
		self.event_lock = Lock()
		self.events = []
		self.wevents = []
		self.running = False
		self.timeslot = timeslot # basic interval
		self.stimer = None
		self.runcount = 1

	def __repr__(self):
		return ",".join(["%s:%s" % (e.name, e.until_next_secs) for e in self.events])

	def start(self):
		logging.info("start...sys_gardD...on: %s" % DT.now())
		self.running = True
		self.sort()
		self.e0 = None
		#self.wait_amount = self.e0.until_next_secs if self.e0 else 0
		self.nextwaiting = self.timeslot
		self.runner()

	def stop(self):
		self.running = False
		if self.stimer and self.stimer.is_alive():
			self.stimer.cancel()
			print("sysgard timer stoped!")

	def _rerun(self, n_wait=0, a_wait=0):
		#print(f"rerun next event: {self.e0}")
		if a_wait > 0:
			self.wait_amount = a_wait
		self.stimer = Timer(n_wait or self.nextwaiting, self.runner)
		self.stimer.start()

	def runner(self):
		#print(self.e0)
		self.runcount += 1
		if self.running is False:
			logging.info("Runing is Over!")
			return
		if self.e0 is None and len(self.events) == 0:
			#print("empty events")
			#Don't set wait amount here
			#self.wait_amount -= self.nextwaiting
			self._rerun()
			return
		elif self.e0 == self.events[0]:
			#print("e0 correct")
			self.wait_amount -= self.nextwaiting
			#self.wait_amount -= self.e0.until_next_secs
		else:
			self.e0 = self.events[0]
			self.wait_amount = self.e0.until_next_secs
		#print("on loop...")
		#print(self.wait_amount)
		e0 = self.e0
		if self.wait_amount <= 0:
			_checkdt = DT.now().replace(microsecond=0)
			logging.info('run event(%s): %s' % (_checkdt, e0.name))
			action = e0.act
			if isinstance(action, Thread):
				try:
					action = e0.act()
					action.start()
				except Exception as E:
					logging.error("action error\t%s" % E)
			elif e0.runmode == event.RUN_THREAD:
				if e0.arg:
					action = Thread(target=e0.act, args=(e0.arg,))
				else:
					action = Thread(target=e0.act, args=())
				try:
					action.start()
				except Exception as E:
					logging.error("action error\t%s" % E)
				# problem: 如果thread event，执行时会直接跳到cal_next(并行)，可能会出现cal_next依赖于执行结果的问题而出现冲突，需要注意
				time.sleep(0.1)
			else:
				try:
					if e0.arg:
						action(e0.arg)
					else:
						action()
				except Exception as E:
					logging.error("action error\t%s" % E)
			if e0.type == event.ETYPE_ONETIME:
				#print("onetime event to remove")
				self.events.pop(0) # real pop out e0
				if len(self.events) > 0:
					self.e0  = self.events[0]
					self.e0.cal_next(_checkdt)
					self.wait_amount = self.e0.until_next_secs
				else:
					self.e0 = None
					self._rerun(self.EMPTY_WAIT)
					return
			else:
				# 内置计数器
				e0.ecount += 1
				# 执行后可能刚执行完的任务实际上依然应该是e0【循环任务】
				# 先顺位但不重排（防止可能的第二个任务由于间隔太短，此时计算后为负值而直接跳到下一次时间）
				self.events.pop(0) # e0已执行，从序列中弹出
				if len(self.events) == 0:
					# 单事件
					#print(f"single event {e0.name} repush.")
					self.events.append(e0)
					self.wait_amount = e0.cal_next(_checkdt, force_next=True)
				else:
					self.quick_insert_event(e0, now=_checkdt)
					# 在qinsert中已经计算了calnext
					self.wait_amount = self.events[0].until_next_secs
				# e0 = self.events[0]
				self.e0 = self.events[0]
				#logging.info(f"next event: {self.e0.name} on {self.e0.until_next_secs} seconds.")
				#print(f"e0 is changed with {self.e0.name} and until_next_secs is {self.e0.until_next_secs}")
				if self.wait_amount < self.timeslot:
					self.nextwaiting = self.wait_amount
				else:
					self.nextwaiting = self.timeslot
		elif self.wait_amount < self.timeslot:
			self.nextwaiting = self.wait_amount
		else:
			self.nextwaiting = self.timeslot
		#logging.info(["%s:%s" % (e.name, e.until_next_secs) for e in self.events])
		#logging.info(f"wait: {self.wait_amount} to event: {self.e0.name}")
		# looper
		self._rerun()

	def get_event(self, ename):
		for e in self.events:
			if e.name == ename:
				return e

	def list_events(self, gettime=False):
		# just event names
		if gettime:
			return [{"name": e.name, "ntime": e.until_next_secs} for e in self.events]
		return [e.name for e in self.events]

	def bind_arg(self, ename, arg):
		e = self.get_event(ename)
		if e:
			e.bind_arg(arg)
			return True
		else:
			raise ValueError("event[%s] not found." % ename)

	def drop_event(self, ename):
		for i in range(len(self.events)):
			if self.events[i].name == ename:
				self.events.pop(i)
				logging.info(f"event {ename} is droped.")
				return True

	def quick_insert_event(self, event, now=None):
		# 快速将事件插入，用于事件重新回列表，前提：列表已经正序，只需要计算排在event之后的第一个事件即可
		# if sametime: 直接计算到下一次
		# BUG： 前一个event qinsert时，头2个均为0；到头1执行后重新qinsert，则由于2者均为0，导致头1（前一步的头2）计算出大数，应当避免此情况出现
		now = now or DT.now().replace(microsecond=0)
		now_tstamp = now.timestamp()
		etime = event.cal_next(now, now_tstamp, force_next=True)
		for i in range(len(self.events)):
			e = self.events[i]
			# less then time-slot will be ignore
			if e.until_next_secs < self.timeslot:
				continue
			e.cal_next(now, now_tstamp)
			#print("%s<%s> - %s<%s>" % (event.name, etime, e.name, e.until_next_secs))
			if e.until_next_secs > etime:
				self.events.insert(i, event)
				#logging.info(f"q insert event {event.name} at positioin: {i}")
				#logging.info(self)
				return
		self.events.append(event)
		#logging.info(self)
		#logging.info(f"{event.name} append to events.")

	# update: if len(event) == 0, reset wait_amount
	def add_event(self, event):
		now = DT.now().replace(microsecond=0)
		now_tstamp = now.timestamp()
		etime = event.cal_next(now, now_tstamp)
		logging.info(f"new event {event.name}")
		if len(self.events) == 0:
			self.events.append(event)
			return
		# sort for newest wait time
		self.sort(now, just_recal=True)
		for i in range(len(self.events)):
			e = self.events[i]
			if e.until_next_secs > etime:
				self.events.insert(i, event)
				#logging.info(self)
				return e.until_next_secs
		self.events.append(event)
		logging.info(self)
		return e.until_next_secs

	def add_events(self, *events):
		now = DT.now().replace(microsecond=0)
		now_tstamp = now.timestamp()
		self.sort(now, just_recal=True)
		for e in events:
			etime = e.cal_next(now, now_tstamp)
			insert = False
			for i in range(len(self.events)):
				_e = self.events[i]
				if _e.until_next_secs > etime:
					self.events.insert(i, e)
					insert = True
					break
			if insert is False:
				self.events.append(e)
		logging.info(f"new event {e.name}")
		logging.info(self)

	def sort(self, dtime=None, just_recal=False):
		# sort events
		# @just_recal: just re calculate for the newest time_wait of all events
		#print("sorting events!")
		if len(self.events) == 0:
			return None
		dtime = (dtime or DT.now()).replace(microsecond=0)
		dtime_tstamp = dtime.timestamp()
		if just_recal:
			for e in self.events:
				# 跳过小于timeslot的值/跳过小于1的值
				if e.until_next_secs < 1:
					continue
				e.cal_next(dtime, dtime_tstamp)
			return
		self.events.sort(key=lambda e: e.cal_next(dtime, dtime_tstamp))
		#print(["%s:%s" % (e.name, e.until_next_secs) for e in self.events])
		return self.events[0]
		
	def __str__(self):
		# 注意可能会出现0.0的排在后面的情况，是cal_next时对应数据显示，不影响
		return "\n" + "".join(["name: %s, next_time: %s\n" % (e.name, e.until_next_secs) for e in self.events])
