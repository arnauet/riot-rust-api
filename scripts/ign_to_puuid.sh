#!/usr/bin/env bash
set -euo pipefail

# Uso:
#   scripts/ign_to_puuid.sh igns_euw.txt 
INPUT_FILE="${1:-seeds.txt}"
OUTPUT_FILE="${2:-seeds_euw.txt}"

if [[ ! -f "$INPUT_FILE" ]]; then
  echo "[ERROR] No encuentro el archivo de IGNs: $INPUT_FILE" >&2
  exit 1
fi

# Vaciar / crear el output
> "$OUTPUT_FILE"

while IFS= read -r line; do
  # Saltar líneas vacías o comentadas
  [[ -z "$line" ]] && continue
  [[ "$line" =~ ^# ]] && continue

  # Separar en GAME_NAME y TAG_LINE usando '#'
  game_name="${line%%#*}"
  tag_line="${line#*#}"

  # Trim cutre (por si hay espacios alrededor)
  game_name="$(echo "$game_name" | sed 's/^ *//;s/ *$//')"
  tag_line="$(echo "$tag_line" | sed 's/^ *//;s/ *$//')"

  echo "Resolviendo $game_name#$tag_line..."

  # Versión simple: asumimos que el bin imprime SOLO el PUUID
  puuid=$(cargo run --release -- \
      --game-name "$game_name" \
      --tag-line "$tag_line" \
      2>/dev/null | tr -d '\r\n')

  if [[ -z "$puuid" ]]; then
    echo "  [WARN] No he podido obtener PUUID para: $line" >&2
    continue
  fi

  # Guardamos PUUID + comentario con el IGN original
  echo "$puuid    # $game_name#$tag_line" >> "$OUTPUT_FILE"

done < "$INPUT_FILE"

echo
echo "✓ Seeds guardadas en: $OUTPUT_FILE"

