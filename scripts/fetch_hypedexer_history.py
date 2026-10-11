#!/usr/bin/env python3
"""Archive authenticated public history without changing production ranks.

Keep the credential outside the archive. Only documented API credit headers
are logged. Provider pagination completion is not a chain completeness proof.
"""
import argparse
from datetime import datetime, timezone
import gzip
import hashlib
import json
import math
from pathlib import Path
import sys
import time
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from capital_history import row_identity


def iso(ms):
    return datetime.fromtimestamp(ms / 1000, timezone.utc).isoformat().replace('+00:00', 'Z')


def walk_backward(request, payload, *, limit=500, max_pages=100):
    """Inclusive oldest timestamp avoids dropping a truncated boundary group."""
    rows, pages, seen = [], [], set()
    end = payload['endTime']
    for _ in range(max_pages):
        batch = request({**payload, 'endTime': end, 'limit': limit})
        if not isinstance(batch, list):
            return rows, pages, 'invalid_response'
        pages.append({'endTime': end, 'count': len(batch)})
        if not batch:
            return rows, pages, 'empty_final_page'
        times = [int(r['time']) for r in batch]
        if any(t < payload['startTime'] or t > end for t in times):
            return rows, pages, 'out_of_requested_range'
        for row in batch:
            identity = row_identity(row)
            if identity not in seen:
                rows.append(row)
                seen.add(identity)
        oldest = min(times)
        if oldest == end:
            if len(batch) >= limit:
                return rows, pages, 'timestamp_saturation'
            end -= 1
        else:
            end = oldest
        if end < payload['startTime']:
            return rows, pages, 'range_end'
    return rows, pages, 'page_limit'


class PublicArchiveClient:
    def __init__(self, key, output, max_credits=5000, allow_credit_estimates=False, requests_per_second=0.5):
        self.key = key
        self.output = output
        self.max_credits = max_credits
        self.spent = 0
        self.balance = None
        self.next_request = 0
        self.requests = []
        self.allow_credit_estimates = allow_credit_estimates
        self.estimated_requests = 0
        self.request_interval = 1 / requests_per_second

    def request(self, route, *, query=None, payload=None, reserve=400, estimated_cost=20, retry=0):
        if self.spent + reserve > self.max_credits or (self.balance is not None and self.balance < reserve):
            raise RuntimeError('Configured free credit budget exhausted')
        time.sleep(max(0, self.next_request - time.monotonic()))
        self.next_request = time.monotonic() + self.request_interval
        url = 'https://api.hypedexer.com' + route
        if query:
            url += '?' + urlencode(query)
        headers = {'Authorization': 'Bearer ' + self.key, 'Content-Type': 'application/json'}
        body = json.dumps(payload).encode() if payload is not None else None
        request = Request(url, data=body, headers=headers)
        index = len(self.requests)
        entry = {'url': url, 'payload': payload, 'observedAt': datetime.now(timezone.utc).isoformat()}
        self.requests.append(entry)
        try:
            with urlopen(request, timeout=60) as response:
                raw = response.read()
                entry['status'] = response.status
                entry['creditCost'] = response.headers.get('X-Credit-Cost')
                entry['creditBalance'] = response.headers.get('X-Credit-Balance')
        except HTTPError as exc:
            entry['status'] = exc.code
            entry['retryAfter'] = exc.headers.get('Retry-After')
            # Record only known quota fields, redacting a reflected credential.
            try:
                error = json.loads(exc.read())
            except (ValueError, UnicodeError):
                error = {}
            for name in ('error', 'message', 'remaining_credits', 'plan'):
                if name in error:
                    entry[name] = str(error[name]).replace(self.key, '[key hidden]')
            if exc.code == 429 and error.get('error') != 'credit_limit_reached' and retry < 3:
                try:
                    pause = float(entry['retryAfter'] or 30)
                except ValueError:
                    pause = 30
                if 0 <= pause <= 60:
                    self.next_request = time.monotonic() + max(1, pause)
                    return self.request(route, query=query, payload=payload, reserve=reserve,
                                        estimated_cost=estimated_cost, retry=retry + 1)
            raise RuntimeError('Hypedexer HTTP ' + str(exc.code)) from None
        path = self.output / f'response-{index:05}.json.gz'
        with gzip.open(path, 'wb') as out:
            out.write(raw)
        entry['file'] = path.name
        entry['sha256'] = hashlib.sha256(path.read_bytes()).hexdigest()
        if entry['creditCost'] is None:
            if not self.allow_credit_estimates:
                raise RuntimeError('Missing credit headers; response saved, further requests stopped')
            cost = max(estimated_cost, reserve)
            entry['creditAccounting'] = 'console_price_estimate_not_actual_spend'
            self.estimated_requests += 1
        else:
            cost = int(entry['creditCost'])
            entry['creditAccounting'] = 'reported_response_header'
        balance = int(entry['creditBalance']) if entry['creditBalance'] is not None else None
        if cost < 0 or (balance is not None and balance < 0):
            raise RuntimeError('Invalid credit headers; further requests stopped')
        self.spent += cost
        self.balance = balance
        return json.loads(raw)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--capital', required=True, type=Path)
    p.add_argument('--output', required=True, type=Path)
    p.add_argument('--key-file', required=True, type=Path)
    p.add_argument('--max-credits', type=int, default=5000)
    p.add_argument('--fill-limit', type=int, default=1000)
    p.add_argument('--allow-documented-credit-estimates', action='store_true',
                   help='Only use after verifying Free plan with no overage in the console')
    p.add_argument('--resume', action='store_true')
    args = p.parse_args()
    if not 1 <= args.max_credits <= 5000 or not 1 <= args.fill_limit <= 1000:
        p.error('Use at most 5000 credits and at most 1000 fills per page')
    key = args.key_file.read_text().strip().removeprefix('HYPEDEXER_API_KEY=')
    if not key or '\n' in key:
        p.error('Invalid credential file')
    if args.output.exists() and any(args.output.iterdir()) and not args.resume:
        p.error('Use a new output directory to avoid overwriting raw responses')
    args.output.mkdir(parents=True, exist_ok=True)
    source = json.loads((args.capital / 'manifest.json').read_text())
    client = PublicArchiveClient(key, args.output, args.max_credits,
                                args.allow_documented_credit_estimates)
    manifest = {'provider': 'Hypedexer', 'startMs': source['startMs'], 'endMs': source['endMs'],
                'requests': client.requests, 'wallets': [], 'productionImportPerformed': False,
                'chainCoverageVerified': False}
    if args.resume:
        manifest = json.loads((args.output / 'manifest.json').read_text())
        if (manifest['startMs'], manifest['endMs']) != (source['startMs'], source['endMs']):
            p.error('Cannot resume a different window')
        client.requests = manifest['requests']
        client.spent = manifest.get('creditsAccounted', 0)
        client.estimated_requests = manifest.get('estimatedRequestCount', 0)
        client.balance = manifest.get('creditsRemaining')
        manifest.pop('stoppedReason', None)
    def save():
        manifest.update({'creditsSpent': None if client.estimated_requests else client.spent,
                         'creditsAccounted': client.spent, 'estimatedRequestCount': client.estimated_requests,
                         'creditsRemaining': client.balance})
        (args.output / 'manifest.json').write_text(json.dumps(manifest, indent=2, allow_nan=False))
    try:
        if 'coverage' not in manifest:
            manifest['coverage'] = client.request('/info', payload={'type': 'dataCoverage'}, estimated_cost=1)
        save()
        # First obtain capital and ledger for every address, before fills can
        # consume the shared quota. Preserve book scope; never join automatically.
        for wallet in source['wallets']:
            address = wallet['address']
            item = next((w for w in manifest['wallets'] if w['address'] == address), None)
            if item is None:
                item = {'address': address, 'accountModeNow': wallet['accountMode'], 'errors': {}}
                manifest['wallets'].append(item)
            for label, route, query in [
                ('equity', f'/users/{address}/equity-history', {'window': 'all', 'interval': '1h'}),
                ('calendar', f'/users/{address}/pnl-calendar', {'window': 'all'}),
            ]:
                if label in item and label not in item['errors']:
                    continue
                try:
                    data = client.request(route, query=query)
                    item[label] = data
                    item['errors'].pop(label, None)
                except RuntimeError as exc:
                    item['errors'][label] = str(exc)
                    if 'HTTP 4' not in str(exc) or 'HTTP 429' in str(exc):
                        raise
                finally:
                    save()
            payload = {'type': 'userNonFundingLedgerUpdates', 'user': address,
                       'startTime': source['startMs'], 'endTime': source['endMs']}
            if item.get('ledger', {}).get('paginationStatus') not in ('empty_final_page', 'range_end'):
                rows, pages, status = walk_backward(lambda body: client.request('/info', payload=body,
                                                    reserve=52, estimated_cost=52), payload)
                item['ledger'] = {'rows': rows, 'pages': pages, 'paginationStatus': status,
                                  'chainCoverageVerified': False}
            save()
            print(json.dumps({'address': address, 'stage': 'capital', 'ledgerRows': len(item['ledger']['rows']),
                              'creditsAccounted': client.spent}), flush=True)
        params = {'start_time': iso(source['startMs']), 'end_time': iso(source['endMs']),
                  'limit': args.fill_limit, 'order': 'ASC'}
        active = []
        for item in manifest['wallets']:
            if 'fills' not in item:
                item['fills'] = {'rowCount': 0, 'pages': [], 'paginationStatus': 'not_started',
                                 'chainCoverageVerified': False}
            if item['fills']['paginationStatus'] == 'provider_end':
                continue
            query = dict(params)
            if item['fills'].get('nextCursor'):
                query['cursor'] = item['fills']['nextCursor']
            elif item['fills']['pages']:
                raise RuntimeError('Cannot resume a fill history without a saved next cursor')
            cursors = {page['cursor'] for page in item['fills']['pages'] if page.get('cursor')}
            active.append((item, query, cursors))
        # Rotate between wallets so one large history cannot take all credits.
        while active:
            pending = []
            for item, query, cursors in active:
                fills = item['fills']
                data = client.request(f"/fills/user/{item['address']}", query=query,
                                      # Live responses charge 2 + ceil(rows/10),
                                      # above both public pricing descriptions.
                                      reserve=max(20, 2 + math.ceil(args.fill_limit / 10)),
                                      estimated_cost=2 + math.ceil(args.fill_limit / 10))
                if data.get('success') is not True or not isinstance(data.get('data'), list):
                    fills['paginationStatus'] = 'invalid_response'
                    raise RuntimeError('Invalid fills envelope')
                fills['rowCount'] += len(data['data'])
                fills['pages'].append({'file': client.requests[-1]['file'], 'rows': len(data['data']),
                                       'cursor': query.get('cursor')})
                fills['advertisedCount'] = data.get('total_count')
                if data.get('has_more') is False:
                    fills['paginationStatus'] = 'provider_end'
                    save()
                    print(json.dumps({'address': item['address'], 'stage': 'fills',
                                      'rows': fills['rowCount'], 'creditsAccounted': client.spent}), flush=True)
                    continue
                cursor = data.get('next_cursor')
                if data.get('has_more') is not True or not cursor or cursor in cursors:
                    fills['paginationStatus'] = 'invalid_cursor'
                    save()
                    raise RuntimeError('Invalid/repeated fills cursor')
                cursors.add(cursor)
                query['cursor'] = cursor
                fills['nextCursor'] = cursor
                fills['paginationStatus'] = 'in_progress'
                save()
                pending.append((item, query, cursors))
            active = pending
            print(json.dumps({'stage': 'fill_round', 'remainingWallets': len(active),
                              'totalFills': sum(w['fills']['rowCount'] for w in manifest['wallets']),
                              'creditsAccounted': client.spent}), flush=True)
    except Exception as exc:
        manifest['stoppedReason'] = str(exc)
        print(json.dumps({'stoppedReason': str(exc), 'creditsSpent': client.spent}), flush=True)
    finally:
        save()


if __name__ == '__main__':
    main()
