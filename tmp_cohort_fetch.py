import json
import urllib.request

D = chr(36)

QUERY = f"""query GetPnlCohort({D}id: String!, {D}limit: Int!, {D}offset: Int!, {D}sortBy: CohortTraderSortInput) {{
  analytics {{
    pnlCohort(id: {D}id) {{
      cohortInfo {{ id label range emoji }}
      totalTraders
      topTraders(limit: {D}limit, offset: {D}offset, sortBy: {D}sortBy) {{
        totalCount
        hasMore
        traders {{
          address
          accountValue
          perpPnl
          copyScore
          displayName
          tag
          label
          verified
          totalNotional
          longNotional
          shortNotional
          lastTradeAt
          positions {{ coin size notionalSize unrealizedPnl entryPrice }}
        }}
      }}
    }}
  }}
}}"""

payload = {
    "query": QUERY,
    "variables": {
        "id": "extremely_profitable",
        "limit": 100,
        "offset": 0,
        "sortBy": None,
    },
    "operationName": "GetPnlCohort",
}

req = urllib.request.Request(
    "https://api.hyperdash.com/graphql",
    data=json.dumps(payload).encode(),
    headers={"Content-Type": "application/json"},
)

try:
    with urllib.request.urlopen(req, timeout=60) as resp:
        data = json.load(resp)
    print(json.dumps(data))
except urllib.error.HTTPError as exc:
    print(exc.read().decode())
