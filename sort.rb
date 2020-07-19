# frozen_string_literal: true

require 'redis'
require 'securerandom'

conn = Redis.new

def new_id
  SecureRandom.uuid
end

def new_ts
  day = rand(1..10)
  hour = rand(1..24)
  minute = rand(1..60)
  second = rand(1..60)
  (Time.now + (day * hour * minute * second)).to_i
end

def random_score
  rand(1..100)
end

# User holds user specific data.
class User
  attr_accessor :id, :email, :score, :created_at, :updated_at

  def initialize
    @id = new_id
    @score = random_score
    @created_at = new_ts
    @updated_at = new_ts
  end

  def to_s
    "email: #{email}, score: #{score}, created_at: #{created_at}, updated_at: #{updated_at}"
  end
end

def save_user(conn, user)
  index = "user:#{user.id}"
  conn.hmset(index,
             'email', user.email,
             'score', user.score,
             'created_at', user.created_at,
             'updated_at', user.updated_at)
end

def load_user(conn, id)
  data = conn.hgetall("user:#{id}")
  user = User.new
  user.id = id
  user.email = data['email']
  user.score = data['score']
  user.created_at = data['created_at']
  user.updated_at = data['updated_at']
  user
end

def save_app_user(conn, app_id)
  index = "app:#{app_id}:users"

  10.times do |i|
    user = User.new
    user.email = "user#{i}@example.com"
    save_user(conn, user)
    conn.zadd(index, user.created_at, user.id)
  end
end

def print(users)
  users.each do |user|
    puts user
  end
end

def list_app_user_by_created_at(conn, app_id, direction)
  users = []
  result = if direction == 'desc'
             conn.zrevrange("app:#{app_id}:users", 0, -1)
           else
             conn.zrange("app:#{app_id}:users", 0, -1)
           end
  result.each do |id|
    users << load_user(conn, id)
  end
  users
end

def list_app_user_by_updated_at(conn, app_id, direction)
  users = []
  conn.sort("app:#{app_id}:users", order: direction, by: 'user:*->updated_at').each do |id|
    user = load_user(conn, id)
    users << user
  end
  users
end

def list_app_user_by_email(conn, app_id, direction)
  users = []
  conn.sort("app:#{app_id}:users", order: "alpha #{direction}", by: 'user:*->email').each do |id|
    user = load_user(conn, id)
    users << user
  end
  users
end

def list_app_user_by_score(conn, app_id, direction)
  users = []
  conn.sort("app:#{app_id}:users", order: direction, by: 'user:*->score').each do |id|
    user = load_user(conn, id)
    users << user
  end
  users
end

def list_app_user(conn, app_id, sort_by, direction)
  if sort_by == 'created_at'
    list_app_user_by_created_at(conn, app_id, direction)
  elsif sort_by == 'updated_at'
    list_app_user_by_updated_at(conn, app_id, direction)
  elsif sort_by == 'email'
    list_app_user_by_email(conn, app_id, direction)
  elsif sort_by == 'score'
    list_app_user_by_score(conn, app_id, direction)
  end
end

app_id = new_id

save_app_user(conn, app_id)

puts 'order by created_at...'
print(list_app_user(conn, app_id, 'created_at', 'asc'))

puts "\norder by updated_at..."
print(list_app_user(conn, app_id, 'updated_at', 'asc'))

puts "\norder by email..."
print(list_app_user(conn, app_id, 'email', 'desc'))

puts "\norder by score..."
print(list_app_user(conn, app_id, 'score', 'desc'))
