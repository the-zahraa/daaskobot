-- db/schema.sql
-- Multi-tenant tables for Telegram Analytics & Management Bot
CREATE TABLE tenants (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  name text NOT NULL,
  owner_user_id bigint NOT NULL, -- Telegram user ID of tenant owner
  created_at timestamptz DEFAULT now()
);

CREATE TABLE users (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id uuid REFERENCES tenants(id) ON DELETE CASCADE,
  tg_user_id bigint NOT NULL,
  username text,
  first_name text,
  last_name text,
  phone text, -- E.164 if provided
  country text,
  language text DEFAULT 'en',
  is_premium boolean DEFAULT false,
  created_at timestamptz DEFAULT now(),
  UNIQUE (tenant_id, tg_user_id)
);

CREATE TABLE chats (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id uuid REFERENCES tenants(id) ON DELETE CASCADE,
  tg_chat_id bigint NOT NULL, -- group or channel id
  title text,
  type text, -- 'group'|'supergroup'|'channel'
  is_active boolean DEFAULT true,
  created_at timestamptz DEFAULT now(),
  UNIQUE (tenant_id, tg_chat_id)
);

CREATE TABLE invite_links (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id uuid REFERENCES tenants(id) ON DELETE CASCADE,
  chat_id uuid REFERENCES chats(id) ON DELETE CASCADE,
  invite_link text NOT NULL,
  campaign_name text,
  created_by bigint, -- tg_user_id
  created_at timestamptz DEFAULT now()
);

CREATE TABLE join_events (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id uuid REFERENCES tenants(id) ON DELETE CASCADE,
  chat_id uuid REFERENCES chats(id) ON DELETE CASCADE,
  tg_user_id bigint,
  invite_link_id uuid REFERENCES invite_links(id),
  event_type text, -- 'join'|'leave'
  join_at timestamptz DEFAULT now()
);

CREATE TABLE subscriptions (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id uuid REFERENCES tenants(id) ON DELETE CASCADE,
  tg_user_id bigint,
  plan text,
  currency text,
  amount numeric,
  provider text, -- 'stars'|'crypto'|'stripe' etc.
  provider_payload jsonb,
  started_at timestamptz DEFAULT now(),
  expires_at timestamptz
);

CREATE TABLE payments (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id uuid REFERENCES tenants(id) ON DELETE CASCADE,
  tg_user_id bigint,
  amount numeric,
  currency text,
  method text,
  provider_payload jsonb,
  status text,
  created_at timestamptz DEFAULT now()
);
