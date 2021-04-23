using Confluent.Kafka;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Newtonsoft.Json;
using OrderMicroservice.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace OrderMicroservice.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class ProducerController : ControllerBase
    {
        private ProducerConfig producerConfig;
        public ProducerController(ProducerConfig config) {
            producerConfig = config;
        }
        [HttpPost("send")]
        public async Task<ActionResult> Get(string topic, [FromBody] Employee employee)
        {
            string serializeEmployee = JsonConvert.SerializeObject(employee);
            using (var producer=new ProducerBuilder<Null, string>(producerConfig).Build())
            {
                await producer.ProduceAsync(topic, new Message<Null, string> { Value = serializeEmployee });
                producer.Flush(TimeSpan.FromSeconds(10));
                return Ok(true);
            }
        }

    }
}
