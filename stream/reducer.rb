#!/usr/bin/ruby

count = 0
last_key = nil

STDIN.each_line do |line|

  key, value = line.strip.split("\t")

  if last_key && last_key != key # end of list
    puts "#{last_key}\t#{count}"
    count = 1
  else
    count += 1
  end

  last_key = key
end

puts "#{last_key}\t#{count}" if last_key
