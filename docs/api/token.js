const jwt = require('jsonwebtoken');

const METABASE_SITE_URL = process.env.METABASE_SITE_URL || "http://localhost:3000";
const METABASE_SECRET_KEY = process.env.METABASE_SECRET_KEY;

export default function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', 'https://kamcioramcio.github.io');
  res.setHeader('Access-Control-Allow-Methods', 'GET,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'X-CSRF-Token, X-Requested-With, Accept, Accept-Version, Content-Length, Content-MD5, Content-Type, Date, X-Api-Version');

  if (req.method === 'OPTIONS') {
    return res.status(200).end();
  }

  try {
    const payload = {
      resource: { dashboard: 2 },
      params: {},
      exp: Math.round(Date.now() / 1000) + (10 * 60)
    };

    const token = jwt.sign(payload, METABASE_SECRET_KEY);
    const iframeUrl = METABASE_SITE_URL + "/embed/dashboard/" + token + "#bordered=true&titled=true";

    res.status(200).json({
      token: token,
      metabaseUrl: METABASE_SITE_URL,
      iframeUrl: iframeUrl
    });
  } catch (error) {
    console.error("Error generating token:", error);
    res.status(500).json({ error: error.message });
  }
}