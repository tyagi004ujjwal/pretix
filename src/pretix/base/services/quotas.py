from collections import Counter, defaultdict
from datetime import timedelta, datetime

from django.conf import settings
from django.db.models import Max, Q, Count, Sum, Func, F
from django.dispatch import receiver
from django.utils.timezone import now
from django_scopes import scopes_disabled

from pretix.base.models import Event, LogEntry, Quota, OrderPosition, Order, Voucher, WaitingListEntry, CartPosition
from pretix.celery_app import app

from ..signals import periodic_task


def calculate_availabilities(quotas, now_dt: datetime = None, count_waitinglist=True, ignore_closed=False,
                             analyze=False):

    """
    Computes the availability of a set of quotas. If analyze=False, a dictionary mapping quotas to tuples of
    is returned. If analyze=True, a dictionary mapping quotas to counter objects with availability components is
    returned.
    """
    # Early-outs
    result = {}
    size_left = Counter()
    for q in quotas:
        if q.closed and not ignore_closed and not analyze:
            result[q] = Quota.AVAILABILITY_ORDERED, 0
        elif q.size is None and not analyze:
            result[q] = Quota.AVAILABILITY_OK, None
        else:
            size_left[q] = q.size

    if not analyze and not any(q not in result for q in quotas):
        return result

    now_dt = now_dt or now()
    analyze_result = defaultdict(Counter)

    ids_to_quotas = {q.pk: q for q in quotas}
    events = {q.event_id for q in quotas}

    q_items = Quota.items.through.objects.filter(
        quota_id__in=[q.pk for q in quotas if q not in result]
    ).values('quota_id', 'item_id')
    item_to_quota = defaultdict(list)
    for m in q_items:
        item_to_quota[m['item_id']].append(ids_to_quotas[m['quota_id']])

    q_vars = Quota.variations.through.objects.filter(
        quota_id__in=[q.pk for q in quotas if q not in result]
    ).values('quota_id', 'itemvariation_id')
    var_to_quota = defaultdict(list)
    for m in q_vars:
        var_to_quota[m['itemvariation_id']].append(ids_to_quotas[m['quota_id']])

    # TODO: store to cache

    # Count paid and pending orders
    op_lookup = OrderPosition.objects.filter(
        order__status=[Order.STATUS_PAID, Order.STATUS_PENDING],
        order__event_id__in=events,
        subevent_id__in={q.subevent_id for q in quotas if q not in result}
    ).filter(
        Q(
            Q(variation_id__isnull=True) &
            Q(item_id__in={i['item_id'] for i in q_items if ids_to_quotas[i['quota_id']] not in result})
        ) | Q(variation_id__in={i['itemvariation_id'] for i in q_vars if ids_to_quotas[i['quota_id']] not in result})
    ).order_by().values('order__status', 'item_id', 'subevent_id', 'variation_id').annotate(c=Count('*'))
    for line in sorted(op_lookup, key=lambda li: li['order__status'], reverse=True):
        if line['variation_id']:
            qs = var_to_quota[line['variation_id']]
        else:
            qs = item_to_quota[line['item_id']]
        for q in qs:
            if q.subevent_id == line['subevent_id'] and q not in result:
                size_left[q] -= line['c']
                if analyze:
                    analyze_result[q]['order_' + line['order__status']] += line['c']
                elif size_left[q] <= 0:
                    if line['order__status'] == Order.STATUS_PAID:
                        result[q] = Quota.AVAILABILITY_GONE, 0
                    else:
                        result[q] = Quota.AVAILABILITY_ORDERED, 0

    if not analyze and not any(q not in result for q in quotas):
        return result

    if 'sqlite3' in settings.DATABASES['default']['ENGINE']:
        func = 'MAX'
    else:  # NOQA
        func = 'GREATEST'

    # Count blocking vouchers
    v_lookup = Voucher.objects.filter(
        Q(event_id__in=events) &
        Q(subevent_id__in={q.subevent_id for q in quotas if q not in result}) &
        Q(block_quota=True) &
        Q(Q(valid_until__isnull=True) | Q(valid_until__gte=now_dt)) &
        Q(
            Q(
                Q(variation_id__isnull=True) &
                Q(item_id__in={i['item_id'] for i in q_items if ids_to_quotas[i['quota_id']] not in result})
            ) | Q(
                variation_id__in={i['itemvariation_id'] for i in q_vars if ids_to_quotas[i['quota_id']] not in result}
            ) | Q(
                quota_id__in=[q.pk for q in quotas]
            )
        )
    ).order_by().values('subevent_id', 'item_id', 'quota_id', 'variation_id').annotate(
        free=Sum(Func(F('max_usages') - F('redeemed'), 0, function=func))
    )
    for line in v_lookup:
        if line['variation_id']:
            qs = var_to_quota[line['itemvariation_id']]
        elif line['item_id']:
            qs = item_to_quota[line['item_id']]
        else:
            qs = [ids_to_quotas[line['quota_id']]]
        for q in qs:
            if q.subevent_id == line['subevent_id'] and q not in result:
                size_left[q] -= line['free']
                if analyze:
                    analyze_result[q]['vouchers'] += line['free']
                elif size_left[q] <= 0:
                    result[q] = Quota.AVAILABILITY_ORDERED, 0

    if not analyze and not any(q not in result for q in quotas):
        return result

    # Count waitinglist
    if count_waitinglist:
        w_lookup = WaitingListEntry.objects.filter(
            Q(event_id__in=events) &
            Q(voucher__isnull=True) &
            Q(subevent_id__in={q.subevent_id for q in quotas if q not in result}) &
            Q(
                Q(variation_id__isnull=True) &
                Q(item_id__in={i['item_id'] for i in q_items if ids_to_quotas[i['quota_id']] not in result})
            ) | Q(variation_id__in={i['itemvariation_id'] for i in q_vars if ids_to_quotas[i['quota_id']] not in result})
        ).order_by().values('item_id', 'subevent_id', 'variation_id').annotate(c=Count('*'))
        for line in w_lookup:
            if line['variation_id']:
                qs = var_to_quota[line['variation_id']]
            else:
                qs = item_to_quota[line['item_id']]
            for q in qs:
                if q.subevent_id == line['subevent_id'] and q not in result:
                    size_left[q] -= line['c']
                    if analyze:
                        analyze_result[q]['waitinglist'] += line['c']
                    elif size_left[q] <= 0:
                        result[q] = Quota.AVAILABILITY_ORDERED, 0

        if not analyze and not any(q not in result for q in quotas):
            return result

    # Count cart positions
    cart_lookup = CartPosition.objects.filter(
        Q(event_id__in=events) &
        Q(subevent_id__in={q.subevent_id for q in quotas if q not in result}) &
        Q(expires__gte=now_dt) &
        Q(
            Q(voucher__isnull=True)
            | Q(voucher__block_quota=False)
            | Q(voucher__valid_until__lt=now_dt)
        ) &
        Q(
            Q(variation_id__isnull=True) &
            Q(item_id__in={i['item_id'] for i in q_items if ids_to_quotas[i['quota_id']] not in result})
        ) | Q(variation_id__in={i['itemvariation_id'] for i in q_vars if ids_to_quotas[i['quota_id']] not in result})
    ).order_by().values('item_id', 'subevent_id', 'variation_id').annotate(c=Count('*'))
    for line in cart_lookup:
        if line['variation_id']:
            qs = var_to_quota[line['variation_id']]
        else:
            qs = item_to_quota[line['item_id']]
        for q in qs:
            if q.subevent_id == line['subevent_id'] and q not in result:
                size_left[q] -= line['c']
                if analyze:
                    analyze_result[q]['waitinglist'] += line['c']
                elif size_left[q] <= 0:
                    result[q] = Quota.AVAILABILITY_RESERVED, 0

    if analyze:
        return analyze_result

    for q in quotas:
        if q not in result and size_left[q] > 0:
            result[q] = Quota.AVAILABILITY_OK, size_left[q]

    return result


@receiver(signal=periodic_task)
def build_all_quota_caches(sender, **kwargs):
    refresh_quota_caches.apply_async()


@app.task
@scopes_disabled()
def refresh_quota_caches():
    # Active events
    active = LogEntry.objects.using(settings.DATABASE_REPLICA).filter(
        datetime__gt=now() - timedelta(days=7)
    ).order_by().values('event').annotate(
        last_activity=Max('datetime')
    )
    for a in active:
        try:
            e = Event.objects.using(settings.DATABASE_REPLICA).get(pk=a['event'])
        except Event.DoesNotExist:
            continue
        quotas = e.quotas.filter(
            Q(cached_availability_time__isnull=True) |
            Q(cached_availability_time__lt=a['last_activity']) |
            Q(cached_availability_time__lt=now() - timedelta(hours=2))
        ).filter(
            Q(subevent__isnull=True) |
            Q(subevent__date_to__isnull=False, subevent__date_to__gte=now() - timedelta(days=14)) |
            Q(subevent__date_from__gte=now() - timedelta(days=14))
        )
        for q in quotas:
            q.availability()
