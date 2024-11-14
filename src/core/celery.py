from __future__ import absolute_import, unicode_literals
import os
from celery import Celery
from celery.schedules import timedelta
from django.conf import settings

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'src.core.settings')

app = Celery('src')

app.config_from_object('django.conf:settings', namespace='CELERY')
app.autodiscover_tasks(lambda: settings.INSTALLED_APPS)


app.conf.beat_schedule = {
    'my-periodic-task': {
        'task': 'users.tasks.process_event_outbox',
        'schedule': timedelta(seconds=5),  # runs every 5 seconds
    },
}
