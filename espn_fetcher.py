  
  import requests
  import threading
  import time

  _cache = {}  # game_id -> list of plays
  _games = []  # live games list
  _lock = threading.Lock()

  POLL_INTERVAL = 30

  CFB_URL = "https://site.api.espn.com/apis/site/v2/sports/football/college-football/scoreboard"
  NFL_URL = "https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard"
  SUMMARY_CFB = "https://site.api.espn.com/apis/site/v2/sports/football/college-football/summary"
  SUMMARY_NFL = "https://site.api.espn.com/apis/site/v2/sports/football/nfl/summary"

  def fetch_scoreboard(url):
      try:
          r = requests.get(url, timeout=10)
          r.raise_for_status()
          return r.json().get("events", [])
      except Exception as e:
          print(f"Scoreboard fetch error: {e}")
          return []

  def fetch_plays(game_id, league):
      url = SUMMARY_CFB if league == "cfb" else SUMMARY_NFL
      try:
          r = requests.get(url, params={"event": game_id}, timeout=15)
          r.raise_for_status()
          data = r.json()
          plays_raw = []
          for drive in data.get("drives", {}).get("previous", []):
              for play in drive.get("plays", []):
                  plays_raw.append(map_play(play, data))
          return [p for p in plays_raw if p]
      except Exception as e:
          print(f"Plays fetch error ({game_id}): {e}")
          return []

  def map_play(play, game_data):
      try:
          play_type = play.get("type", {}).get("text", "")
          skip_types = {"End Period", "End of Half", "End of Game", "Coin Toss"}
          if play_type in skip_types:
              return None

          competitors = game_data.get("header", {}).get("competitions", [{}])[0].get("competitors", [])
          home = next((c for c in competitors if c.get("homeAway") == "home"), {})
          away = next((c for c in competitors if c.get("homeAway") == "away"), {})

          home_score_raw = home.get("score", 0)
          away_score_raw = away.get("score", 0)
          home_score = int(home_score_raw.get("value", 0) if isinstance(home_score_raw, dict) else home_score_raw or 0)
          away_score = int(away_score_raw.get("value", 0) if isinstance(away_score_raw, dict) else away_score_raw or 0)

          period = play.get("period", {}).get("number", 0)
          clock = play.get("clock", {}).get("displayValue", "")
          text = play.get("text", "")
          play_id = str(play.get("id", ""))

          start = play.get("start", {})
          down = start.get("down", 0)
          distance = start.get("distance", 0)
          yard_line = start.get("yardLine", 0)

          return {
              "play_id": play_id,
              "play_type": play_type,
              "period": period,
              "clock": clock,
              "text": text,
              "down": down,
              "distance": distance,
              "yard_line": yard_line,
              "home_score": home_score,
              "away_score": away_score,
              "wallclock": play.get("wallclock", "")
          }
      except Exception as e:
          print(f"Map play error: {e}")
          return None

  def poll_all():
      while True:
          new_games = []
          for url, league in [(CFB_URL, "cfb"), (NFL_URL, "nfl")]:
              for event in fetch_scoreboard(url):
                  game_id = event.get("id")
                  status = event.get("status", {}).get("type", {}).get("name", "")
                  comps = event.get("competitions", [{}])[0]
                  competitors = comps.get("competitors", [])
                  home = next((c for c in competitors if c.get("homeAway") == "home"), {})
                  away = next((c for c in competitors if c.get("homeAway") == "away"), {})
                  new_games.append({
                      "game_id": game_id,
                      "league": league,
                      "status": status,
                      "home": home.get("team", {}).get("displayName", ""),
                      "away": away.get("team", {}).get("displayName", ""),
                  })
                  plays = fetch_plays(game_id, league)
                  with _lock:
                      _cache[game_id] = plays

          with _lock:
              _games.clear()
              _games.extend(new_games)

          time.sleep(POLL_INTERVAL)

  def start_poller():
      t = threading.Thread(target=poll_all, daemon=True)
      t.start()

  def get_live_games():
      with _lock:
          return list(_games)

  def get_game_plays(game_id):
      with _lock:
          return _cache.get(game_id, [])
