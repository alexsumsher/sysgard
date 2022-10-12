from threading import Thread, Lock
import datetime
import time
import logging

from .sysg_event import event


DT = datetime.datetime

class sys_gardT(Thread):
	"""
	1. event队列初始化，按照延迟时间排列，先后排序
	2. 新增event计算后，按照延迟时间insert到正确位置
	3. 根据第一位置计算sleep（如果sleep大于一日second，则按照一日second延迟），第一位置延迟小于600s则直接按照对应值sleep，否则提前60s；
		提前60s，重新计算首位的延迟时间，以求一定的精准；
	4. wake up后对首event计算，确认时间到，提取，新线程执行，同时重新计算（按照首event的seconds直接全减，实现快速重算）队列所有delay，所
		执行的event根据类型判断移除还是计算下一次的delay，并根据delay结果放到队列中
	5> fix：同时问题，pre_e0 == _e0，当pre_e0运行后重新计算会导致_e0跳过当前序列到下一次序列
	"""

	def __init__(self, timeslot=20):
		super(sys_gardT, self).__init__()
		self.event_lock = Lock()
		self.events = []
		self.wevents = []
		self.running = False
		self.timeslot = timeslot # basic interval

	def run(self):
		# running enveronment
		logging.info("start...sys_gardT...on: %s" % DT.now())
		self.running = True
		TIME_SLOT = self.timeslot
		wait_amount = 0

		e0 = self.sort()
		wait_amount = e0.until_next_secs if e0 else 0
		nextwaiting = TIME_SLOT
		while self.running is True:
			#e0 = self.events[0] if len(self.events) > 0 else None
			if e0 is None and len(self.events) == 0:
				#print("empty events")
				time.sleep(TIME_SLOT)
				continue
			elif e0 == self.events[0]:
				#print("e0 correct")
				wait_amount -= nextwaiting
			else:
				e0 = self.events[0]
				wait_amount = e0.until_next_secs

			#print("on loop, e0: %s" % e0.name)
			#print(wait_amount)
			if wait_amount <= 0:
				logging.info('run event(%s): %s' % (DT.now(), e0.name))
				action = e0.act
				if isinstance(action, Thread):
					action = e0.act()
					action.start()
				elif e0.runmode == event.RUN_THREAD:
					if e0.arg:
						action = Thread(target=e0.act, args=(e0.arg,))
					else:
						action = Thread(target=e0.act, args=())
					try:
						action.start()
					except Exception as E:
						logging.error("action error\t%s" % E)
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
					self.events.pop(0)
					if len(self.events) > 0:
						e0 = self.events[0]
						try:
							e0.cal_next(DT.now().replace(microsecond=0))
							wait_amount = e0.until_next_secs
						except:
							logging.error(f"Event {e0.name} Failure on Fore_Next_Calculating.")
							e0 = None
							continue
					else:
						e0 = None
						continue
				else:
					e0.ecount += 1
					# 执行后可能刚执行完的任务实际上依然应该是e0【循环任务】
					# 先顺位但不重排（防止可能的第二个任务由于间隔太短，此时计算后为负值而直接跳到下一次时间）
					_checkdt = DT.now().replace(microsecond=0)
					self.events.pop(0) # e0已执行，从序列中弹出
					# 注意可能出现pre_e0.until_next_secs和下一个其实是同时的情况，在计算和重排前先确认[小于1s算同时]
					if len(self.events) == 0:
						# 单事件
						#print("single event e0: %s" % e0.name)
						self.events.append(e0)
						e0.cal_next(_checkdt, force_next=True)
					else:
						#print("go next with e0: %s" % e0.name)
						self.quick_insert_event(e0, now=_checkdt)
					e0 = self.events[0]
					wait_amount = e0.until_next_secs
					if wait_amount < TIME_SLOT:
						nextwaiting = wait_amount
					else:
						nextwaiting = TIME_SLOT
				#logging.info(f"next event: {e0.name} on {wait_amount} seconds.")
			elif wait_amount < TIME_SLOT:
				nextwaiting = wait_amount
			else:
				nextwaiting = TIME_SLOT
			#logging.info(["%s:%s" % (e.name, e.until_next_secs) for e in self.events])
			#logging.info(f"wait: {wait_amount} to event: {e0.name}")
			time.sleep(nextwaiting)
		logging.info("Running is Over!")

	def stop(self):
		self.running = False
		print("sysGardT will stop in less than %s seconds." % self.timeslot)

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

	def drop_event(self, evt):
		for i in range(len(self.events)):
			if self.events[i].name == evt:
				self.events.pop(i)
				logging.info(f"event {evt} is droped.")
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
			# or less then timeslot/nextwaiting
			if e.until_next_secs < 1:
				continue
			e.cal_next(now, now_tstamp)
			#print("%s<%s> - %s<%s>" % (event.name, etime, e.name, e.until_next_secs))
			if e.until_next_secs > etime:
				self.events.insert(i, event)
				#logging.info(f"q insert event {event.name} at positioin: {i}")
				return
		self.events.append(event)
		#logging.info(f"{event.name} append to events.")

	def add_event(self, event, now=None):
		now = now or DT.now().replace(microsecond=0)
		now_tstamp = now.timestamp()
		etime = event.cal_next(now, now_tstamp)
		logging.info(f"new event {event.name}")
		if len(self.events) == 0:
			self.events.append(event)
			return
		self.sort(now, just_recal=False)
		for i in range(len(self.events)-1):
			e = self.events[i]
			if e.until_next_secs > etime:
				self.events.insert(i, event)
				logging.info(f"insert event {e.name} at positioin: {i}")
				return
		self.events.append(event)
		logging.info(f"append event {e.name}")
		return

	def add_events(self, *events, now=None):
		now = now or DT.now().replace(microsecond=0)
		now_tstamp = now.timestamp()
		self.sort(now, just_recal=True)
		for e in events:
			etime = e.cal_next(now, now_tstamp)
			insert = False
			for i in range(len(self.events)-1):
				_e = self.events[i]
				if _e.until_next_secs > etime:
					self.events.insert(i, e)
					insert = True
					break
			if insert is False:
				self.events.append(e)
			logging.info(f"new event {e.name}")
		logging.info(["%s:%s" % (e.name, e.until_next_secs) for e in self.events])

	def sort(self, dtime=None, just_recal=False):
		# sort events
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
		return "".join(["name: %s, next_time: %s\n" % (e.name, e.until_next_secs) for e in self.events])