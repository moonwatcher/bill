#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import os
import re
import logging
import json
import math
import hashlib
from StringIO import StringIO
from datetime import datetime
from datetime import timedelta
from argparse import ArgumentParser
from subprocess import Popen, PIPE

log_levels = {
    'debug': logging.DEBUG,
    'info': logging.INFO,
    'warning': logging.WARNING,
    'error': logging.ERROR,
    'critical': logging.CRITICAL
}
expression = {
    'csv record header':u'{:<9},{:<17},{:<17},{:<9},{:<9},{:<9}',
    'csv record pattern':u'{:<9},{:<17},{:<17},{:<9},{:<10.2f},{:<10.2f}',
    'current record pattern':u'Current shift started at {:<16} and has been running for {:<16}',
    'datetime format':'%Y-%m-%dT%H:%M:%S.%f',
    'csv datetime format':'%Y-%m-%d %H:%M',
    'date format':'%Y-%m-%d',
    'time delta':{
        'pattern':re.compile('(?:(?P<hours>[0-9]+)h)?(?:(?P<minutes>[0-9]+)m)?(?:(?P<seconds>[0-9]+)s)?(?P<sign>-)?'),
    },
    'epoch': datetime.utcfromtimestamp(0),
}

class Bill(object):
    def __init__(self, env):
        self.log = logging.getLogger('bill')
        self.env = env
        self.config = None
        self.project = None
        
        # load the JSON config file
        if self.env['conf']:
            path = os.path.realpath(os.path.expanduser(os.path.expandvars(self.env['conf'])))
            if os.path.exists(path):
                try:
                    conf = open(path, 'r')
                    stream = StringIO(conf.read())
                    conf.close()
                except IOError as ioerr:
                    self.log.warning(u'Failed to load config file %s', path)
                    self.log.debug(ioerr)
                else:
                    try:
                        self.config = json.load(stream)
                    except ValueError, e:
                        self.log.warning(u'Failed to decode JSON document %s', path)
                        self.log.debug(u'Exception raised %s', unicode(e))
                    else:
                        self.project = {}
                        for k,v in self.config['project'].iteritems():
                            v['name'] = k
                            self.project[k] = ProjectBill(self, v)
            else:
                self.log.warning(u'Could not find configuation file %s', self.env['conf'])
                
    @property
    def valid(self):
        return self.config is not None
    
    
    def load(self):
        for project in self.project.values():
            project.expand()
    
    
    def unload(self):
        for project in self.project.values():
            project.collapse()
    
    
    def start(self):
        for project in self.project.values():
            project.start()
    
    
    def stop(self):
        for project in self.project.values():
            project.stop()
    
    
    def report(self):
        start = None
        if 'from' in self.env:
            start = datetime.strptime(self.env['from'], expression['date format'])
            
        end = None
        if 'to' in self.env:
            end = datetime.strptime(self.env['to'], expression['date format'])
            
        for project in self.project.values():
            project.report(start, end)
    
    def balance(self):
        start = None
        if 'from' in self.env:
            start = datetime.strptime(self.env['from'], expression['date format'])
            
        end = None
        if 'to' in self.env:
            end = datetime.strptime(self.env['to'], expression['date format'])
            
        for project in self.project.values():
            project.balance(start, end)
    
    def pay(self):
        amount = None
        if 'amount' in self.env:
            amount = self.env['amount']
            
        date = None
        if 'date' in self.env:
            date = datetime.strptime(self.env['date'], expression['date format'])
            
        for project in self.project.values():
            project.pay(amount, date)
    


class ProjectBill(object):
    def __init__(self, bill, config):
        self.log = logging.getLogger('project')
        self.bill = bill
        self.config = config
        self.node = None
        self._current = None
        self._history = None
        self.volatile = False
    
    
    def varify_directory(self, path):
        result = True
        try:
            if not os.path.exists(path):
                self.log.debug(u'Creating directory %s', path)
                os.makedirs(path)
        except OSError as err:
            self.log.error(unicode(err))
            result = False
        return result
    
    
    def expand(self):
        if self.config and 'db' in self.config:
            path = os.path.realpath(os.path.expanduser(os.path.expandvars(self.config['db'])))
            if os.path.exists(path):
                try:
                    conf = open(path, 'r')
                    stream = StringIO(conf.read())
                    conf.close()
                except IOError as ioerr:
                    self.log.warning(u'Failed to load database for %s', self.name, path)
                    self.log.debug(u'Exception raised %s', unicode(ioerr))
                else:
                    try:
                        self.node = json.load(stream)
                    except ValueError, valerr:
                        self.log.warning(u'Failed to decode JSON database for %s', self.name)
                        self.log.debug(u'Exception raised %s', unicode(valerr))
                    else:
                        self._history = []
                        if 'current' in self.node:
                            self._current = Shift(self, self.node['current'])
                            
                        if 'history' in self.node:
                            for e in self.node['history']:
                                if e['type'] == 'shift':
                                    self._history.append(Shift(self, e))
                                    
                                elif e['type'] == 'payment':
                                    self._history.append(Payment(self, e))
                                    
                        # sort the history
                        if self.env['sort']:
                            self.volatile = True
                            self.log.debug(u'Sorting history')
                            self._history.sort(key=lambda event: event.order)
            
            else: self.node = {}
    
    
    def collapse(self):
        if self.volatile:
            self.node = { 'history':[], }
            for shift in self._history:
                self.node['history'].append(shift.node)
                
            if self._current is not None:
                self.node['current'] = self._current.node
            
            path = os.path.realpath(os.path.expanduser(os.path.expandvars(self.config['db'])))
            if self.varify_directory(os.path.dirname(path)):
                self.log.debug(u'Flushing database for %s', self.name)
                try:
                    conf = open(path, 'w')
                    conf.write(self.json)
                    conf.close()
                except IOError as ioerr:
                    self.log.warning(u'Failed to write %s frame index %s', self.name, path)
                    self.log.debug(u'Exception raised %s', unicode(ioerr))
                else:
                    self.volatile = False
    
    
    def select(self):
        query = {}
        
        # Read values from CLI
        if 'time' in self.env:
            query['time'] = datetime.strptime(self.env['time'], expression['datetime format'])
        else:
            query['time'] = datetime.now()
            
        if 'quantize' in self.env:
            query['quantize'] = parse_time_delta(self.env['quantize'])

        if 'offset' in self.env:
            offset = parse_time_delta(self.env['offset'])
            query['time'] = query['time'] + offset
            
        self.log.debug(u'query is %s', unicode(query))
        return query
    
    def start(self):
        if self.current is None:
            query = self.select()
            if query is not None:
                self.volatile = True
                self._current = Shift(self)
                self.current._start = query['time']
                self.current._precision = query['quantize']
                if 'comment' in self.env:
                    self.current.comment = self.env['comment']
            self.log.info(u'Started a shift for project %s at %s', self.name, self.current.start)
        else:
            self.log.error(u'Project %s already has a shift running since %s. You must close it first.', self.name, self.current.start)
    
    def stop(self):
        if self.current is not None:
            query = self.select()
            self.current._end = query['time']
            if 'comment' in self.env:
                self.current.comment = self.env['comment']
            
            self.history.append(self.current)
            current = self.current
            self._current = None
            self.volatile = True
            self.log.info(u'Shift duration %s from %s to %s for project %s.', current.round_duration, current.round_start, current.round_end, self.name)
        else:
            self.log.error(u'Project %s has no running shift. You must start one first.', self.name)
    
    
    def pay(self, amount, date):
        payment = Payment(self, {'amount':amount})
        if date is None:
            payment._date = datetime.now()
        else:
            payment._date = date
        
        self.volatile = True
        self.history.append(payment)
    
    def report(self, start, end):
        total = {
            'duration':timedelta(),
            'shift':0,
            'payment':0,
            'early':None,
            'late':None,
            'labour':0.0,
            'deposit':0.0,
            'balance':0.0,
        }
        for event in self.history:
            if isinstance(event, Shift):
                if (start is None or event.round_start > start) \
                and (end is None or event.round_start < end):
                    if total['early'] is None or event.round_start < total['early']:
                        total['early'] =  event.round_start
                        
                    if total['late'] is None or event.round_end > total['late']:
                        total['late'] =  event.round_end
                        
                    # update totals
                    total['duration'] += event.round_duration
                    total['shift'] += 1
                    total['labour'] += event.value
                    total['balance'] += event.value
                    event.balance = total['balance']
                    
            elif isinstance(event, Payment):
                if (start is None or event.date > start) \
                and (end is None or event.date < end):
                    if total['early'] is None or event.date < total['early']:
                        total['early'] =  event.date
                        
                    if total['late'] is None or event.date > total['late']:
                        total['late'] =  event.date
                        
                    # update totals
                    total['payment'] += 1
                    total['deposit'] += event.value
                    total['balance'] -= event.value
                    event.balance = total['balance']
            
        total['hours'] = total['duration'].total_seconds() / 3600.0
        print u'{:<10}: {}'.format('Name', self.name)
        print u'{:<10}: {}'.format('From', total['early'])
        print u'{:<10}: {}'.format('To', total['late'])
        print u'{:<10}: {}'.format('Payments', total['payment'])
        print u'{:<10}: {}'.format('Shifts', total['shift'])
        print u'{:<10}: {:.2f} hours'.format('Work', total['hours'])
        print u'{:<10}: {:.2f}$'.format('Labour', total['labour'])
        print u'{:<10}: {:.2f}$'.format('Deposit', total['deposit'])
        print u'{:<10}: {:.2f}$'.format('Balance', total['balance'])
        
        if self.current is not None:
            self.current.report()
    
    
    def balance(self, start, end):
        total = {
            'balance':0.0,
        }
        print expression['csv record header'].format (
            u'type',
            u'start',
            u'end',
            u'duration',
            u'amount',
            u'balance',
        )
        for event in self.history:
            if isinstance(event, Shift):
                if (start is None or event.round_start > start) \
                and (end is None or event.round_start < end):
                    # update totals
                    total['balance'] += event.value
                    event.balance = total['balance']
                    event.print_balance()
                    
            elif isinstance(event, Payment):
                if (start is None or event.date > start) \
                and (end is None or event.date < end):
                    # update totals
                    total['balance'] -= event.value
                    event.balance = total['balance']
                    event.print_balance()
    
    
    @property
    def current(self):
        return self._current
    
    
    @property
    def history(self):
        return self._history
    
    
    @property
    def name(self):
        return self.config['name']
    
    
    @property
    def rate(self):
        return self.config['rate']
    
    
    @property
    def env(self):
         return self.bill.env
    
    
    @property
    def json(self):
         return json.dumps(self.node, ensure_ascii=False, sort_keys=True, indent=4,  default=default_json_handler).encode('utf-8')
    


class Event(object):
    def __init__(self, project, node={}):
        self.log = logging.getLogger('event')
        self.project = project
        self.balance = None
        self._node = node
    
    @property
    def type(self):
        return self._node['type']
    
    
    @property
    def node(self):
        result = {}
        return result
    
    
    @property
    def comment(self):
        return ('comment' in self._node and self._node['comment']) or None;
    
    
    @comment.setter
    def comment(self, value):
        self._node['comment'] = value
    
    
    @property
    def env(self):
         return self.project.env
    
    
    @property
    def json(self):
         return json.dumps(self.node, ensure_ascii=False, sort_keys=True, indent=4,  default=default_json_handler).encode('utf-8')
    


class Shift(Event):
    def __init__(self, project, node={}):
        Event.__init__(self, project, node)
        self._start = None
        self._end = None
        self._precision = None
    
    
    
    def print_balance(self):
        print expression['csv record pattern'].format (
            self.type,
            datetime.strftime(self.round_start, expression['csv datetime format']),
            datetime.strftime(self.round_end, expression['csv datetime format']),
            unicode(self.round_duration),
            self.value,
            self.balance
        )
    
    def report(self):
        if self.running:
            print ''
            print expression['current record pattern'].format (
                datetime.strftime(self.start, expression['csv datetime format']),
                unicode(self.duration),
            )
            if self.comment:
                print self.comment
        else:
            print expression['csv record pattern'].format (
                datetime.strftime(self.round_start, expression['csv datetime format']),
                datetime.strftime(self.round_end, expression['csv datetime format']),
                unicode(self.round_duration),
                self.value,
                self.balance
            )
    
    @property
    def running(self):
        return self.start is not None and self.end is None
    
    
    @property
    def node(self):
        result = {}
        result['type'] = 'shift'
        if self.start is not None:
            result['start'] = datetime.strftime(self.start, expression['datetime format'])
        if self.end is not None:
            result['end'] = datetime.strftime(self.end, expression['datetime format'])
        if self.precision is not None:
            result['precision'] = int(self.precision.total_seconds())
        return result
    
    
    @property
    def start(self):
        if self._start is None and 'start' in self._node:
            self._start = datetime.strptime(self._node['start'], expression['datetime format'])
        return self._start
    
    
    @property
    def end(self):
        if self._end is None and 'end' in self._node:
            self._end = datetime.strptime(self._node['end'], expression['datetime format'])
        return self._end
        
    
    @property
    def precision(self):
        if self._precision is None and 'precision' in self._node:
            self._precision = timedelta(seconds=self._node['precision'])
        return self._precision
    
    
    @property
    def duration(self):
        if self.running:
            return datetime.now() - self.start
        elif self.start is not None and self.end is not None:
            return self.end - self.start
        else:
            return None
    
    
    @property
    def value(self):
        return (float(self.round_duration.total_seconds()) / 3600) * self.project.config['rate']
    
    
    @property
    def round_start(self):
        if self.start is not None and self.precision is not None:
            return round_datetime_to_timedelta(self.start, self.precision)
        else:
            return None
    
    
    @property
    def round_end(self):
        if self.end is not None and self.precision is not None:
            return round_datetime_to_timedelta(self.end, self.precision)
        else:
            return None
    
    
    @property
    def round_duration(self):
        if self.running:
            return round_datetime_to_timedelta(datetime.now(), self.precision) - self.start
        elif self.round_start is not None and self.round_end is not None:
            return self.round_end - self.round_start
        else:
            return None
    

    @property
    def order(self):
        return self.start
    


class Payment(Event):
    def __init__(self, project, node={}):
        Event.__init__(self, project, node)
        self._date = None
    
    
    @property
    def node(self):
        result = {}
        result['type'] = 'payment'
        result['amount'] = self.value
        result['date'] = datetime.strftime(self.date, expression['datetime format'])
        return result
    
    
    def print_balance(self):
        print expression['csv record pattern'].format (
            self.type,
            datetime.strftime(self.date, expression['csv datetime format']),
            '',
            '',
            self.value,
            self.balance
        )
    
    
    def report(self):
        print expression['csv record pattern'].format (
            datetime.strftime(self.date, expression['csv datetime format']),
            '',
            '',
            self.value,
            self.balance
        )
    
    
    @property
    def date(self):
        if self._date is None and 'date' in self._node:
            self._date = datetime.strptime(self._node['date'], expression['datetime format'])
        return self._date
    
    
    @property
    def value(self):
        return self._node['amount']
    
    
    @property
    def order(self):
        return self.date
    


def default_json_handler(o):
    result = None
    from bson.objectid import ObjectId
    if isinstance(o, datetime):
        result = datetime.strftime(o, expression['datetime format'])
    if isinstance(o, ObjectId):
        result = str(o)
        
    return result

def parse_time_delta(delta):
    result = None
    if delta is None:
        result = timedelta()
    else:
        match = expression['time delta']['pattern'].search(delta)
        if match is not None:
            o = {}
            minus = False
            for k,v in match.groupdict().iteritems():
                if v:
                    if k == 'sign':
                        if v == '-': minus = True
                    else: o[k] = int(v)
            result = timedelta(**o)
            if minus: result = -result
        
    return result

def round_datetime_to_timedelta(time, quantizer):
    def datetime_to_seconds(time):
        delta = time - expression['epoch']
        return delta.total_seconds()
    
    stamp = datetime_to_seconds(time)
    quant = quantizer.total_seconds()
    result = int(stamp / quant)
    remain = stamp - result * quant
    if int(round(remain/quant)): result += 1
    return datetime.utcfromtimestamp(result * quant)

def decode_cli():
    env = {}

    # -- global arguments for all actions --
    p = ArgumentParser()
    p.add_argument('-v', '--verbosity', metavar='LEVEL', dest='verbosity', default='info',                help='logging verbosity level [default: %(default)s]', choices=log_levels.keys())
    p.add_argument('-c', '--conf',      metavar='PATH',  dest='conf',      default='~/.bill/config.json', help='Path to configuration file [default: %(default)s]')
    p.add_argument('-p', '--project',                    dest='project',   default='wrd',                 help='project to bill')
    
    
    p.add_argument('-s', '--sort',                       dest='sort',      action='store_true')
    
    # application version
    p.add_argument('--version', action='version', version='%(prog)s 0.1')
    
    # -- sub parsers for each action --
    s = p.add_subparsers(dest='action')
    c = s.add_parser( 'start', help='start billing',
        description='TIMESTAMP is given as YYYY-MM-DD HH:MM:SS, DURATION is given as {H}h{M}m{S}s{sign}? or any subset, i.e. 4h34m-'
    )
    c.add_argument('-t', '--time',     metavar='TIMESTAMP', dest='time',                   help='time to start [defualt: now]')
    c.add_argument('-o', '--offset',   metavar='DURATION',  dest='offset',   default='0s', help='offset time to start [default: %(default)s]')
    c.add_argument('-q', '--quantize', metavar='DURATION',  dest='quantize', default='1m', help='round to the nearest time fragment [default: %(default)s]')
    c.add_argument('-m', '--message',  metavar='MESSAGE',   dest='comment', help='comment for shift')

    c = s.add_parser( 'stop', help='stop billing',
        description='TIMESTAMP is given as YYYY-MM-DD HH:MM:SS, DURATION is given as {H}h{M}m{S}s{sign}? or any subset, i.e. 4h34m-'
    )
    c.add_argument('-t', '--time',     metavar='TIMESTAMP', dest='time',                   help='time to stop [defualt: now]')
    c.add_argument('-o', '--offset',   metavar='DURATION',  dest='offset',   default='0s', help='offset time to stop [default: %(default)s]')
    c.add_argument('-m', '--message',  metavar='MESSAGE',   dest='comment', help='comment for shift')

    c = s.add_parser( 'pay', help='pay an amount',
        description='Add a payment record. DATE is given as YYYY-MM-DD.'
    )
    c.add_argument('-m', '--amount', metavar='AMOUNT', type=float, dest='amount',   help='Amount of payment')
    c.add_argument('-d', '--date',   metavar='DATE', dest='date',   help='Date of payment')

    c = s.add_parser( 'report', help='report hours',
        description='DATE is given as YYYY-MM-DD.'
    )
    c.add_argument('-f', '--from', metavar='DATE', dest='from', help='Earliest time to start report')
    c.add_argument('-t', '--to',   metavar='DATE', dest='to',   help='Latest time to report')
    
    c = s.add_parser( 'balance', help='CSV balance sheet',
        description='A CSV balance sheet. DATE is given as YYYY-MM-DD.'
    )
    c.add_argument('-f', '--from', metavar='DATE', dest='from', help='Earliest time to start report')
    c.add_argument('-t', '--to',   metavar='DATE', dest='to',   help='Latest time to report')

    for k,v in vars(p.parse_args()).iteritems():
        if v is not None:
            env[k] = v
    return env
    

def main():
    logging.basicConfig()
    logging.getLogger().setLevel(logging.DEBUG)
    
    env = decode_cli()
    logging.getLogger().setLevel(log_levels[env['verbosity']])
    bill = Bill(env)
    bill.load()
    if bill.valid:
        if env['action'] == 'start':
            bill.start()
        
        if env['action'] == 'stop':
            bill.stop()
            
        if env['action'] == 'report':
            bill.report()
            
        if env['action'] == 'balance':
            bill.balance()
            
        if env['action'] == 'pay':
            bill.pay()
    bill.unload()

if __name__ == '__main__':
    main()
