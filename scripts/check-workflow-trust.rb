#!/usr/bin/env ruby
# frozen_string_literal: true

require "pathname"
require "yaml"

PR_EVENTS = %w[pull_request pull_request_target].freeze
RELEASE_RUNNER = "contabo-ci-release"

def workflow_value(document, key)
  return nil unless document.is_a?(Hash)
  return document[key] if document.key?(key)

  # Psych follows YAML 1.1 and may parse the unquoted key `on` as boolean true.
  return document[true] if key == "on" && document.key?(true)

  nil
end

def load_workflow(path)
  document = YAML.safe_load(
    path.read,
    permitted_classes: [],
    permitted_symbols: [],
    aliases: false,
    filename: path.to_s
  )
  document || {}
rescue Psych::Exception => error
  raise "Cannot safely parse #{path}: #{error.message}"
end

def pr_triggered?(document)
  trigger = workflow_value(document, "on")

  case trigger
  when String
    PR_EVENTS.include?(trigger)
  when Array
    trigger.any? { |event| PR_EVENTS.include?(event.to_s) }
  when Hash
    trigger.keys.any? { |event| PR_EVENTS.include?(event.to_s) }
  else
    false
  end
end

def contains_release_runner?(value)
  case value
  when String
    value.include?(RELEASE_RUNNER)
  when Array
    value.any? { |item| contains_release_runner?(item) }
  when Hash
    value.any? do |key, item|
      contains_release_runner?(key.to_s) || contains_release_runner?(item)
    end
  else
    false
  end
end

def local_reusable_path(root, workflow_directory, reference)
  return nil unless reference.start_with?("./")

  candidate = root.join(reference.delete_prefix("./")).cleanpath
  directory_prefix = "#{workflow_directory}/"
  unless candidate.to_s.start_with?(directory_prefix)
    raise "Local reusable workflow escapes #{workflow_directory}: #{reference}"
  end
  raise "Local reusable workflow does not exist: #{reference}" unless candidate.file?

  candidate
end

def privileged_path(path, root, workflow_directory, stack)
  raise "Reusable workflow cycle: #{(stack + [path]).join(' -> ')}" if stack.include?(path)

  document = load_workflow(path)
  jobs = workflow_value(document, "jobs")
  return nil unless jobs.is_a?(Hash)

  jobs.each_value do |job|
    next unless job.is_a?(Hash)
    return "#{path}: job contains #{RELEASE_RUNNER}" if contains_release_runner?(job)

    reusable = workflow_value(job, "uses")
    next unless reusable.is_a?(String)

    local_path = local_reusable_path(root, workflow_directory, reusable)
    unless local_path
      return "#{path}: external reusable workflow cannot be trust-audited (#{reusable})"
    end

    nested = privileged_path(
      local_path,
      root,
      workflow_directory,
      stack + [path]
    )
    return "#{path} -> #{nested}" if nested
  end

  nil
end

root = Pathname.new(ARGV.fetch(0, ".")).expand_path
workflow_directory = root.join(".github/workflows")
unless workflow_directory.directory?
  warn "Workflow directory does not exist: #{workflow_directory}"
  exit 2
end

workflow_paths = workflow_directory.children.select do |path|
  path.file? && %w[.yml .yaml].include?(path.extname)
end.sort

violations = []
workflow_paths.each do |path|
  document = load_workflow(path)
  next unless pr_triggered?(document)

  finding = privileged_path(path, root, workflow_directory, [])
  violations << finding if finding
end

unless violations.empty?
  warn "PR-triggered workflow trust violations:"
  violations.each { |violation| warn "- #{violation}" }
  exit 1
end

puts "Workflow runner trust contract: PASS"
