# Tether Workers

Deploy the Tether Mind API worker to your Cloudflare account.

## Setup

1. **Get your IDs** from the Tether installer success page

2. **Edit `wrangler.toml`** - replace the placeholder values:
   - `YOUR_DATABASE_ID` with your Database ID
   - `YOUR_SOULFILES_KV_ID` with your Soulfiles KV ID
   - `YOUR_DISCORD_KV_ID` with your Discord KV ID
   - Replace `YOURNAME` with your companion's name

3. **Install and deploy:**
   ```bash
   npm install
   npx wrangler login
   npx wrangler deploy
   ```

## Support

- Discord: https://discord.gg/BCfvvj5J
- Email: aibetweenuswebsite@gmail.com
