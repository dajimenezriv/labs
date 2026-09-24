# Auth

- [Introduction](#introduction)
  - [IdP (Identity Provider)](#idp-identity-provider)
- [Passwords](#passwords)
- [JWT (JSON Web Token)](#jwt-json-web-token)
  - [HMAC (Hash-based Message Authentication Code)](#hmac-hash-based-message-authentication-code)
  - [Asymmetric](#asymmetric)
  - [Structure](#structure)
    - [Header](#header)
    - [Payload](#payload)
    - [Signature](#signature)
  - [Issues](#issues)
- [Session](#session)
- [JWT vs Session](#jwt-vs-session)
- [OAuth and OIDC (OpenID Connect)](#oauth-and-oidc-openid-connect)
  - [Single Sign On](#single-sign-on)
- [Storage](#storage)
  - [Cookie](#cookie)
  - [Local Storage](#local-storage)
- [Real implementations](#real-implementations)
  - [Most companies (5-30 services)](#most-companies-5-30-services)
  - [Growing (30-200 services)](#growing-30-200-services)
    - [Client Credentials](#client-credentials)
  - [Large scale](#large-scale)
- [Microservices](#microservices)
  - [How to validate?](#how-to-validate)

## Introduction

- **Authentication**: tells a server that we are a certain user (`userID=145`).
- **Authorization**: tells a server that we are allowed to perform certain actions (`scopes=orders:read,orders:write`).

We need a way to tell the server our `userID` and `scopes` in a secure way. Options:

- **Session**: a standard `sessionID`. The session data is stored server-side.
- **Stateful JWT**: JWT `accessToken` that contains the `sessionID`. The session data is stored server-side.
- **Stateless JWT**: JWT `accessToken` that contains the session data, encoded directly into the token.
- **OAuth (authorization)**: only used to communicate with external services. A user logs in into that service (Google for example), the service sends us a JWT `accessToken` (and usually a `refreshToken`) that we can use to access their APIs like Google Drive.
- **OIDC (authentication)**: built on top of OAuth, returns also an `idToken` that contains the `externalUserID` from that service and other fields like the `email`.

### IdP (Identity Provider)

In case we don't want to implement our own authentication/authorization service we can use an existing one: Microsoft Entra ID, Auth0, Keycloak, Clerk.

They provide features like:

- Password reset, email verification and multi-factor authentication.
- Scopes.
- Account linking — same email arrives via Google and via password. Merge? Reject?
- Session revocation and device management ("log out everywhere").

When a user sends the `accessToken` or `sessionID` we validate it in the provider and we obtain the `userID` and `scopes`.

```go
// Example of how to validate an `accessToken` in Microsoft Entra ID using JWT RSA.
// Not production code, just a demostration ignoring errors.
var (
  tenantID = os.Getenv("TENANT_ID")
  clientID = os.Getenv("CLIENT_ID")
  // Microsoft has also a common one: https://login.microsoftonline.com/common/discovery/v2.0/keys
  jwksURI  = fmt.Sprintf("https://login.microsoftonline.com/%s/discovery/v2.0/keys", tenantID)
  issuer = fmt.Sprintf("https://login.microsoftonline.com/%s/v2.0", tenantID)
)

type JWKS struct {
  Keys []struct {
    Kid string   `json:"kid"`
    X5c []string `json:"x5c"`
  } `json:"keys"`
}

type Claims struct {
  OID   string   `json:"oid"`
  Roles []string `json:"roles"`
  Email string   `json:"email"`
  Name  string   `json:"name"`
  jwt.RegisteredClaims
}

func publicKey(kid string) any {
  // Cache by kid, refetch on unknown kid.
  res, _ := http.Get(jwksURI)
  defer res.Body.Close()

  var jwks JWKS
  json.NewDecoder(res.Body).Decode(&jwks)

  for _, k := range jwks.Keys {
    if k.Kid == kid {
      x5c := k.X5c[0] // X.509 certificate chain (RFC 7517)
      der, _ := base64.StdEncoding.DecodeString(x5c)
      // -----BEGIN CERTIFICATE-----
      // <x5c>
      // -----END CERTIFICATE-----
      cert, _ := x509.ParseCertificate(der)
      return cert.PublicKey
    }
  }
  return nil
}

func Authenticate(r *http.Request, scopes []string) *Claims {
  raw := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")

  claims := &Claims{}
  jwt.ParseWithClaims(raw, claims,
    func(t *jwt.Token) (any, error) {
      return publicKey(t.Header["kid"].(string)), nil
    },
    jwt.WithValidMethods([]string{"RS256"}),
    jwt.WithAudience(clientID),
    jwt.WithIssuer(issuer),
  )

  if len(scopes) == 0 {
    return claims
  }
  for _, s := range scopes {
    if slices.Contains(claims.Roles, s) {
      return claims
    }
  }
  return nil
}
```

## Passwords

Argon2id is the current recommendation from OWASP, RFC 9016. Basically the hashing is when the database is stolen they need to spend a lot of resources on brute force the hash.

| Algorithm | Year | Resists GPU | Resists ASIC/FPGA | Notes |
|---|---|---|---|---|
| Argon2id | 2015 | Yes | Yes (memory-hard) | Hybrid of Argon2i/d — side-channel + GPU resistance |
| scrypt | 2009 | Yes | Yes (memory-hard) | Good, but tuning is fiddlier; less analyzed than Argon2 |
| bcrypt | 1999 | Partially | No | 72-byte input limit, 4 KB fixed memory |
| PBKDF2 | 2000 | No | No | CPU-only, trivially parallelized on GPUs |

## JWT (JSON Web Token)

- Can be verified and trusted (integrity) because it's signed using a secret (HMAC algorithm) or a public/private key pair using RSA or ECDSA (see Microsoft Entra ID example).
- Can also be encrypted to protect the data.
- `accessTokens` should have a TTL of 5-15 min.
- `refreshTokens` should have a TTL of 30-90 days.

### HMAC (Hash-based Message Authentication Code)

This is a symmetric key (we share a key with the provider).

Stripe webhooks are signed using HMAC SHA-256. Shopify uses this too.

```
Stripe-Signature:
t=1492774577 (timestamp)
v1=5257a869e7ecebeda32affa62cdca3fa51cad7e77a0e56ff536d0ce8e108d8bd (signature)
v0=6ffbb59b2300aae63f272406069a9788598b792a944a07aba816edb039989a39 (old signature)
```

### Asymmetric

More recommended in microservices, otherwise each service will need the shared key to validate the JWT, which implies that they can also generate JWTs with any scope.

Instead, an identity service will hold the private key and the services hold the public key. In case we want to rotate the private key, we can implement the JWKS endpoint that servers the public keys. Each private/public key is labelled with a `kid`, and we know which `kid` to use based on the JWT header.

### Structure

`Header.Payload.Signature`

#### Header

This JSON is Base64Url encoded to form the first part of the JWT. The same happens with the other parts.

```json
{
  "alg": "HS256", // (HMAC SHA256 or RSA).
  "typ": "JWT"
}
```

#### Payload

It contains the claims. Three types of claims (registered, public and private).

```json
// Registered claims
{
  "sub": "110248495921238986420", // Subject: userID
  "iat": 1754300000, // Issued at
  "exp": 1754303600, // Expiration time
  "iss": "https://accounts.google.com", // Issuer of the token
  "aud": "407408718192.apps.googleusercontent.com", // Audience
  "jti": "abcd1234-5678-efgh-ijkl-9012mnopqrst" // Unique identifier for this token. When we want to keep a list of revoked tokens we use this
}
```

#### Signature

To create the signature part you have to take the encoded header, the encoded payload, a secret, the algorithm specified in the header, and sign that.

`token = HMACSHA256(base64UrlEncode(header) + "." + base64UrlEncode(payload), secret)`
`Authorization: Bearer <token>`

### Issues

A `refreshToken` should only be used once, so we keep a table in DB to track all refresh tokens that were already used (reuse detection). However, if a user performs 2 consequent refresh requests, the second with throw a 401 and will logout the user. To fix this we introduce a grace period to be able to use the same `refreshToken` for 10s. However, this also introduces new problems, like 2 different `refreshTokens` live or making the `refreshToken` computation deterministic.

## Session

- **Server Response**: when you log in, the server sends a cookie like `Set-Cookie: __Host-session=4f8a3b2c9e...; Secure; HttpOnly`.
- **Client Storage**: your browser (or whatever client) saves this token. Usually saved in cookies or local storage.
- **Subsequent Requests**: your browser automatically sends that ID back in the request header.
- **Server Response**: when you log out, the server sends a cookie like `Set-Cookie: __Host-session=; Secure; HttpOnly`.

We need to store in our server the session data. Every request must hit the DB/Cache.

- Sessions can be signed too. Otherwise an attacker could try to brute force until find a `sessionID`.

```go
type LoginOutput struct {
  SetCookie http.Cookie `header:"Set-Cookie"`
}

func login(ctx context.Context, in *LoginInput) *LoginOutput {
  return &LoginOutput{
    SetCookie: http.Cookie{
      Name:     "__Host-session",
      Value:    "userToken",
      Path:     "/",
      Expires:  "expiresAt",
      HttpOnly: true,
      Secure:   true, // Only sent over HTTPS.
      SameSite: http.SameSiteLaxMode, // Protects partially from CSRF (Since the cookie is always added, is an attack that deceives the browser to execute an action from that confident app). This blocks cross-site POST requests, but doesn't block cross-site GET requests. To fully protect it still needs a CSRF token.
    },
  }
}

type LogoutInput struct {
  Session string `cookie:"__Host-session"`
}

type LogoutOutput struct {
  SetCookie http.Cookie `header:"Set-Cookie"`
}

func (s *Server) logout(ctx context.Context, in *LogoutInput) *LogoutOutput {
  return &LogoutOutput{SetCookie: http.Cookie{
    Name:     "__Host-session",
    Value:    "",
    Path:     "/",
    MaxAge:   -1,
    HttpOnly: true,
    Secure:   true,
    SameSite: http.SameSiteLaxMode,
  }}
}
```

## JWT vs Session

| | Session | Stateless JWT |
|-|-|-|
| **Where state lives** | Server (DB/cache) | Inside the token |
| **Cost per request** | Lookup | Signature verification (CPU only) |
| **Revocation** | Instant — delete the row | Not possible until `exp`, unless you keep a `jti` deny-list (which reintroduces the lookup you were avoiding) |
| **Claim freshness** | Always current | Stale until `exp`. Role removed at 14:00 is still valid at 14:10 |
| **Size on the wire** | ~32 bytes | 500B–2KB, on every request |
| **Cross-domain / mobile / M2M** | Awkward (cookies are origin-bound) | Natural |
| **Main attack surface** | CSRF (cookie is sent automatically) | XSS if stored in JS-reachable storage |

Technically sessions are safer because the server can instantly revoke or modify session data. However, if a session is stolen it last longer. We can rotate them, but then we introduce issues of race condition on rotation like the JWTs have. Usually we can check if a session is used by checking that the IP/User-Agent is always the same.

In the end we start introducing features from one to the other and they become similar.

## OAuth and OIDC (OpenID Connect)

OAuth (1.0) is the previous version and OAuth 2.0 is the current one.

By default we just receive the `accessToken` (with the scopes that we have asked for) and maybe a `refreshToken`. We can use OIDC on top of Auth 2.0 to make is also Authentication and receive the `idToken`.

```go
// Example: we need to read the Google Drive of a user
Browser → Google Universal Login (IdP)
        → redirect to your app with ?code
        → exchange code + PKCE verifier for accessToken / idToken / refreshToken
        → call Google Drive API with `Authorization: Bearer <accessToken>`
```

It would be similar as if the user grabs his `accessToken` and `refreshToken` manually and gives them to us.

### Single Sign On

Nowadays uses JWT and it's very similar to OAuth 2.0. In Internxt we did something similar with the login (it's not OAuth 2.0, because the desktop app is from the same provider). We were opening a server in the desktop app and opening the web app, the web was sending his JWTs to the localhost server and we were asking for a new set of JWTs.

## Storage

### Cookie

- Small piece of data stored directly in the user's web browser.
- Limited to 4KB.
- Automatically sent with all HTTP requests.
- Security: less secure unless flags like `HttpOnly`. To prevent cross-site scripting (XSS) attacks, HttpOnly cookies are inaccessible from the document.cookie JavaScript API.
- Suited for non sensitive data, like theme, language.

### Local Storage

- 5–10 MB per origin, strings only, synchronous API (blocks the main thread).
- Not sent automatically — you attach it yourself (Authorization: Bearer). Immune to CSRF by construction.
- Readable by any JS running on the page → one XSS and the token is exfiltrated. An HttpOnly cookie is not readable, so XSS can only use it from the victim's browser, not steal it.
- Scoped per origin, not shared across subdomains (cookies can be, with Domain=).
- Not available in service workers or during SSR.
- SPA best practice: refreshToken in an HttpOnly cookie, accessToken in a JS variable in memory. Never localStorage for tokens if you have the choice.

## Real implementations

### Most companies (5-30 services)

- Buy the IdP, Auth0 for example.
- A gateway like Ngnix validates the JWT against the IdP's JWKS endpoint (see Microsoft Entra ID example) and get `X-User-ID` and `X-Scopes`.
- Internal services trust in `X-User-ID` since everything it's inside a private VPC.
- `refreshTokens` should use reuse detection.

### Growing (30-200 services)

- Shared auth middleware library.
- Services start to verify signature by themselves instead of trusting headers.
- Machine-to-machine calls get their own `client_credentials` identities instead of piggybacking on user tokens. This can happen when a request is made because of a cronjob and not because of a user action.

#### Client Credentials

We register in the identity service a `client_id` + `client_secret` and the service exchanges them for a normal `accessToken`.

```
POST /oauth/token
Content-Type: application/x-www-form-urlencoded
Authorization: Basic base64(client_id:client_secret)

grant_type=client_credentials&scope=invoices:read invoices:write
```

| user token | client token |
| --- | --- |
| sub = user id | sub = client id |
| roles/permissions derived from the user | scope granted to the client |
| refresh token issued | no refresh token |

- We need to cache the token until shortly before `exp`. In go we can use `golang.org/x/oauth2/clientcredentials` does the fetch-and-cache transparently.
- The problem is the shared secret. There are alternatives like `private_key_jwt` or `mTLS` because the secret never travels. Outside the scope.

### Large scale

The issue that we have is that with the previous methods we need a secret to either sign the JWT or to ask for an `accessToken` to the IdP. For example, to get an `accessToken` in Microsoft Entra ID we need an `AZURE_AD_CLIENT_SECRET`.

## Microservices

- Nginx uses a field called `auth_request`: performs a subrequest to an internal endpoint that returns 200/401, plus headers that nginx copies onto the upstream request.
- Envoy calls it `ext_authz`.
- Traefik calls it `forward_auth`.

```nginx
location = /_auth {
    internal;
    proxy_pass              http://identity/verify;
    proxy_pass_request_body off;
    proxy_set_header        Content-Length "";
    proxy_set_header        X-Original-URI $request_uri;
    proxy_set_header        X-Original-Method $request_method;
}

location /api/ {
    auth_request     /_auth;
    auth_request_set $user_id $upstream_http_x_user_id;
    auth_request_set $scopes  $upstream_http_x_scopes;

    proxy_set_header X-User-Id $user_id;
    proxy_set_header X-Scopes  $scopes;
    proxy_set_header Cookie    "";

    proxy_pass http://orders;
}
```

### How to validate?

```
Session → API Gateway (`auth_request`)
        → Identity service
        → DB lookup → Is network private?
                    → YES → Send X-User-ID and X-Scopes in plain text
                    → NO  → Generate an internal JWT token
                          → Validate in each service
```

```
JWT → API Gateway
    → Validate in each service
```
