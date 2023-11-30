using MessageBroker.Data;
using MessageBroker.Models;
using Microsoft.EntityFrameworkCore;

var builder = WebApplication.CreateBuilder(args);
//dotnet tool update --global dotnet-ef dotnet tool install --global dotnet-ef dotnet ef database update

// Add services to the container.
builder.Services.AddDbContext<AppDbContext>(options =>
{
    options.UseSqlite("Data Source=MessageBroker.db");
});

builder.Services.AddControllers();
// Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseHttpsRedirection();

//Create Topic
app.MapPost("api/topics", async (AppDbContext context, Topic topic) =>
{
    await context.Topics.AddAsync(topic);

    await context.SaveChangesAsync();

    return Results.Created($"api/topics/{topic.Id}", topic);
});

//Return all Topics
app.MapGet("api/topics", async (AppDbContext context) =>
{
    var topics = await context.Topics.ToListAsync();

    return Results.Ok(topics);
});

//Publish Message 
app.MapPost("api/topics/{id:int}/messages", async (AppDbContext context, int id, Message message) =>
{
    bool topics = await context.Topics.AnyAsync(topic => topic.Id == id);

    if (!topics)
    {
        return Results.NotFound("Topics not found");
    }
    
    //If no subscriptions do this
    var subs = context.Subscriptions.Where(sub => sub.TopicId == id);

    if (!subs.Any())
    {
        return Results.NotFound("There are no subs to this topic");
    }

    foreach (var sub in subs)
    {
        var msg = new Message
        {
            TopicMessage = message.TopicMessage,
            SubscriptionId = sub.Id,
            ExpiresAfter = message.ExpiresAfter,
            MessageStatus = message.MessageStatus
        };

        await context.Messages.AddAsync(msg);
    }

    await context.SaveChangesAsync();

    return Results.Ok("Message has been published");
});

//Create Subscription
app.MapPost("api/topics/{id:int}/subscriptions", async (AppDbContext context, int id, Subscription sub) =>
{
    bool topics = await context.Topics.AnyAsync(topic => topic.Id == id);

    if (!topics)
    {
        return Results.NotFound("Topics not found");
    }

    sub.TopicId = id;

    await context.Subscriptions.AddAsync(sub);

    await context.SaveChangesAsync();

    return Results.Created($"api/topics/{id}/subscriptions/{sub.Id}", sub);
});

//Get Subscriber Messages
app.MapGet("api/subscriptions/{id:int}/messages", async (AppDbContext context, int id) =>
{
    var subbed = await context.Subscriptions.AnyAsync(sub => sub.Id == id);

    if (!subbed)
    {
        return Results.NotFound("Not found");
    }

    var messages = context.Messages.Where(msg => msg.SubscriptionId == id && msg.MessageStatus != "SENT");

    if (!messages.Any())
    {
        return Results.NotFound("No new messages");
    }

    foreach (var msg in messages)
    {
        msg.MessageStatus = "REQUESTED";
    }

    await context.SaveChangesAsync();

    return Results.Ok(messages);
});

//Ack Messages for Subscriber
app.MapPost("api/subscriptions/{id:int}/messages", async (AppDbContext context, int id, int[] confs) =>
{
    var subbed = await context.Subscriptions.AnyAsync(sub => sub.Id == id);

    if (!subbed)
    {
        return Results.NotFound("subscription not found");
    }

    if (confs.Length <= 0)
    {
        return Results.BadRequest();
    }

    int count = 0;

    foreach (int index in confs)
    {
        var msg = await context.Messages.FirstOrDefaultAsync(m => m.Id == index);

        if (msg != null)
        {
            msg.MessageStatus = "SENT";

            await context.SaveChangesAsync();
            count++;
        }
    }

    return Results.Ok($"Acknowledged {count}/{confs.Length} messages");

});

app.UseAuthorization();

app.MapControllers();

app.Run();