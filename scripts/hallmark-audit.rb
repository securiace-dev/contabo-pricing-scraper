#!/usr/bin/env ruby
# frozen_string_literal: true

# Hallmark-inspired static UI release gate for the WHMCS-native VPS suite.
# This deliberately checks durable, machine-verifiable constraints. Visual
# hierarchy and responsive composition are still reviewed from rendered
# fixtures and recorded in docs/HALLMARK_REVIEW.md.

require "pathname"

ROOT = Pathname.new(__dir__).parent
SCOPES = [
  ROOT.join("whmcs-module/modules/addons/contabo_pricing"),
  ROOT.join("whmcs-module/modules/servers/securiacevps"),
  ROOT.join("whmcs-module/templates/orderforms/securiace-vps")
].freeze
CSS_FILES = [
  ROOT.join("whmcs-module/modules/addons/contabo_pricing/assets/app.css"),
  ROOT.join("whmcs-module/modules/servers/securiacevps/assets/clientarea.css"),
  ROOT.join("whmcs-module/modules/servers/securiacevps/assets/tokens.css"),
  ROOT.join("whmcs-module/templates/orderforms/securiace-vps/custom.css")
].freeze
STAMPED_CSS = [
  ROOT.join("whmcs-module/modules/addons/contabo_pricing/assets/app.css"),
  ROOT.join("whmcs-module/modules/servers/securiacevps/assets/clientarea.css"),
  ROOT.join("whmcs-module/templates/orderforms/securiace-vps/custom.css")
].freeze

failures = []
passes = []

def relative(path)
  path.relative_path_from(ROOT).to_s
end

def source_files
  SCOPES.flat_map do |scope|
    scope.glob("**/*").select do |path|
      path.file? && %w[.php .tpl .js .css].include?(path.extname) &&
        !path.each_filename.to_a.include?("vendor")
    end
  end
end

def check_no_match(files, pattern, label, failures, passes)
  matches = []
  files.each do |path|
    path.each_line.with_index(1) do |line, number|
      matches << "#{relative(path)}:#{number}: #{line.strip}" if line.match?(pattern)
    end
  end
  if matches.empty?
    passes << label
  else
    failures << "#{label}\n    #{matches.join("\n    ")}"
  end
end

def srgb_channel(value)
  channel = value / 255.0
  channel <= 0.04045 ? channel / 12.92 : ((channel + 0.055) / 1.055)**2.4
end

def hex_luminance(hex)
  value = hex.delete_prefix("#")
  value = value.chars.map { |c| c * 2 }.join if value.length == 3
  red, green, blue = value.scan(/../).map { |part| part.to_i(16) }
  0.2126 * srgb_channel(red) + 0.7152 * srgb_channel(green) + 0.0722 * srgb_channel(blue)
end

def linear_to_srgb(value)
  value <= 0.0031308 ? 12.92 * value : 1.055 * (value**(1.0 / 2.4)) - 0.055
end

def oklch_luminance(lightness, chroma, hue)
  radians = hue * Math::PI / 180
  a = chroma * Math.cos(radians)
  b = chroma * Math.sin(radians)

  l_prime = lightness + 0.3963377774 * a + 0.2158037573 * b
  m_prime = lightness - 0.1055613458 * a - 0.0638541728 * b
  s_prime = lightness - 0.0894841775 * a - 1.2914855480 * b
  l_value = l_prime**3
  m_value = m_prime**3
  s_value = s_prime**3

  red_linear = 4.0767416621 * l_value - 3.3077115913 * m_value + 0.2309699292 * s_value
  green_linear = -1.2684380046 * l_value + 2.6097574011 * m_value - 0.3413193965 * s_value
  blue_linear = -0.0041960863 * l_value - 0.7034186147 * m_value + 1.7076147010 * s_value

  red = [[linear_to_srgb(red_linear), 0].max, 1].min
  green = [[linear_to_srgb(green_linear), 0].max, 1].min
  blue = [[linear_to_srgb(blue_linear), 0].max, 1].min
  0.2126 * srgb_channel((red * 255).round) +
    0.7152 * srgb_channel((green * 255).round) +
    0.0722 * srgb_channel((blue * 255).round)
end

def contrast(first, second)
  lighter, darker = [first, second].sort.reverse
  (lighter + 0.05) / (darker + 0.05)
end

files = source_files

check_no_match(
  files,
  /(?:\sstyle\s*=|<style\b|\son(?:click|submit|change|input|load|error|mouseover|keydown)\s*=|\.style\.)/i,
  "no inline style blocks, style attributes, event handlers, or DOM style mutation",
  failures,
  passes
)
check_no_match(
  CSS_FILES,
  /transition\s*:\s*all\b/i,
  "no transition-all declarations",
  failures,
  passes
)
check_no_match(
  CSS_FILES,
  /border-(?:left|right)\s*:\s*(?:[2-9]|\d{2,})px/i,
  "no thick coloured side-stripe decoration",
  failures,
  passes
)
check_no_match(
  files,
  /(?:data-cb-open-drawer|cb-row-clickable)/,
  "no fake clickable table rows or dead drawer controls",
  failures,
  passes
)

missing_stamps = STAMPED_CSS.reject { |path| path.read.include?("Hallmark · macrostructure:") }
if missing_stamps.empty?
  passes << "Hallmark macrostructure stamp present on admin, client, and order surfaces"
else
  failures << "missing Hallmark macrostructure stamp: #{missing_stamps.map { |path| relative(path) }.join(', ')}"
end

raw_design_values = []
CSS_FILES.each do |path|
  path.each_line.with_index(1) do |line, number|
    stripped = line.strip
    next if stripped.start_with?("/*", "*", "*/", "//")
    if stripped.match?(/(?:#[0-9a-f]{3,8}\b|rgba?\(|oklch\()/i) && !stripped.match?(/--[\w-]+\s*:/)
      raw_design_values << "#{relative(path)}:#{number}: raw colour outside a token declaration"
    end
    if stripped.match?(/font-family\s*:/i) &&
       !stripped.match?(/--[\w-]+\s*:/) &&
       !stripped.match?(/font-family\s*:\s*(?:var\(|inherit)/i)
      raw_design_values << "#{relative(path)}:#{number}: raw font stack outside a token declaration"
    end
  end
end
if raw_design_values.empty?
  passes << "colours and font stacks are tokenised"
else
  failures << "untokenised design values\n    #{raw_design_values.join("\n    ")}"
end

spacing_failures = []
CSS_FILES.each do |path|
  path.each_line.with_index(1) do |line, number|
    line.scan(/\b(?:margin(?:-(?:top|right|bottom|left))?|padding(?:-(?:top|right|bottom|left))?|gap|row-gap|column-gap)\s*:\s*([^;]+)/i).each do |match|
      match.first.scan(/(-?\d+(?:\.\d+)?)px/i).flatten.each do |value|
        next if (value.to_f % 4).zero?
        spacing_failures << "#{relative(path)}:#{number}: #{value}px is outside the 4px spacing scale"
      end
    end
  end
end
if spacing_failures.empty?
  passes << "margin, padding, and gap pixel values follow the 4px scale"
else
  failures << "spacing-scale violations\n    #{spacing_failures.join("\n    ")}"
end

nested_cards = []
SCOPES.flat_map { |scope| scope.glob("**/*.tpl") }.each do |path|
  stack = []
  path.read.scan(/<\/?([a-z][\w:-]*)([^>]*)>/im).each do |tag, attributes|
    raw = Regexp.last_match(0)
    if raw.start_with?("</")
      stack.pop
      next
    end
    classes = attributes[/\bclass\s*=\s*["']([^"']+)["']/i, 1].to_s.split
    if classes.include?("cb-card") && stack.any? { |entry| entry.include?("cb-card") }
      nested_cards << relative(path)
    end
    stack << classes unless raw.end_with?("/>") || %w[area base br col embed hr img input link meta param source track wbr].include?(tag.downcase)
  end
end
if nested_cards.empty?
  passes << "no nested admin card-on-card composition"
else
  failures << "nested .cb-card composition: #{nested_cards.uniq.join(', ')}"
end

hex_pairs = {
  "admin body on surface" => ["#1a1d24", "#fffdfa", 4.5],
  "admin muted on surface" => ["#5c6373", "#fffdfa", 4.5],
  "admin on accent" => ["#fffaf2", "#b45309", 4.5],
  "admin focus on surface" => ["#075985", "#fffdfa", 3.0],
  "order body on surface" => ["#13233a", "#fffdfa", 4.5],
  "order muted on surface" => ["#5b6b80", "#fffdfa", 4.5],
  "order on accent" => ["#fffaf2", "#0c6b58", 4.5],
  "order focus on surface" => ["#1769aa", "#fffdfa", 3.0]
}
hex_pairs.each do |label, (foreground, background, threshold)|
  ratio = contrast(hex_luminance(foreground), hex_luminance(background))
  if ratio >= threshold
    passes << format("%s contrast %.2f:1", label, ratio)
  else
    failures << format("%s contrast %.2f:1 is below %.1f:1", label, ratio, threshold)
  end
end

client_colours = {
  ink: oklch_luminance(0.23, 0.025, 255),
  muted: oklch_luminance(0.49, 0.025, 255),
  surface: oklch_luminance(0.99, 0.008, 90),
  accent: oklch_luminance(0.53, 0.14, 245),
  safe: oklch_luminance(0.53, 0.12, 155),
  warn: oklch_luminance(0.58, 0.14, 75),
  danger: oklch_luminance(0.52, 0.19, 28),
  focus: oklch_luminance(0.61, 0.19, 250),
  soft_amber: oklch_luminance(0.95, 0.045, 80),
  soft_red: oklch_luminance(0.95, 0.035, 28)
}
client_pairs = {
  "client body on surface" => [:ink, :surface, 4.5],
  "client muted on surface" => [:muted, :surface, 4.5],
  "client surface on accent" => [:surface, :accent, 4.5],
  "client safe on surface" => [:safe, :surface, 4.5],
  "client warning on soft amber" => [:warn, :soft_amber, 3.0],
  "client danger on soft red" => [:danger, :soft_red, 4.5],
  "client focus on surface" => [:focus, :surface, 3.0]
}
client_pairs.each do |label, (foreground, background, threshold)|
  ratio = contrast(client_colours.fetch(foreground), client_colours.fetch(background))
  if ratio >= threshold
    passes << format("%s contrast %.2f:1", label, ratio)
  else
    failures << format("%s contrast %.2f:1 is below %.1f:1", label, ratio, threshold)
  end
end

passes.each { |message| puts "[PASS] #{message}" }
unless failures.empty?
  failures.each { |message| warn "[FAIL] #{message}" }
  warn "Hallmark static audit: FAIL (#{failures.length} invariant#{failures.length == 1 ? '' : 's'})"
  exit 1
end

puts "Hallmark static audit: PASS (#{passes.length} invariants)"
