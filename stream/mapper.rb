#!/usr/bin/ruby

STDIN.each_line do |l|

  line = l.strip
  
  unless line.empty?
    words = line.split(/\W+/)
    words.each do |word|
      puts "#{word.strip}\t1"
    end
  end
end
