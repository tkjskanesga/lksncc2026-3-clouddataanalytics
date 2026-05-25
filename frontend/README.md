# NusaCommerce Analytics Dashboard — Frontend

Static web dashboard to visualize the NusaCommerce Analytics Platform results.

## Stack

- **DaisyUI 4** (Tailwind CSS component library) — via CDN
- **Chart.js 4** — bar, doughnut, horizontal bar charts
- **Lucide Icons** — via CDN
- **Vanilla JS** — no build step required

## Deployment (AWS Amplify)

The dashboard is deployed to AWS Amplify as a static web app. See Task 25 in the module.

```bash
# Package and deploy
zip -j /tmp/frontend.zip frontend/index.html
# Then use Amplify manual deployment (create-deployment → upload → start-deployment)
```

## Configuration

After opening the dashboard URL in a browser, click **Configure** in the top navigation bar and fill in:

- **API Gateway Base URL**: `https://{api-id}.execute-api.us-east-1.amazonaws.com/prod`
- **API Key**: API key value from Usage Plan `nusa-usage-plan`

Click **Save & Connect** — the dashboard will automatically load data from all endpoints.

---

## Dashboard Widgets

| Widget | Data Source | Endpoint |
|--------|-------------|----------|
| GMV / Orders / Active Users | DynamoDB | `GET /metrics/realtime` |
| Sales by Category (Bar) | Redshift | `GET /analytics/sales` |
| Conversion Funnel (Horizontal Bar) | Athena | `GET /analytics/funnel` |
| Seller Segments (Doughnut) | Redshift | `GET /analytics/recommendations?type=sellers` |
| User Segments (Doughnut) | Redshift | `GET /analytics/recommendations?type=users` |

## CORS

Ensure API Gateway has CORS enabled on all endpoints so the browser can call the API from the Amplify domain.
