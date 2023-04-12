package assignment4.postservlet;


import assignment4.config.datamodel.ResponseMsg;
import assignment4.config.util.Pair;
import assignment4.config.datamodel.SwipeDetails;
import java.util.logging.Logger;
import javax.servlet.*;
import javax.servlet.http.*;
import javax.servlet.annotation.*;
import java.io.IOException;
import com.google.gson.Gson;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

@WebServlet(name = "assignment4.postservlet.SwipeServlet", value = "/swipe")
public class SwipeServlet extends HttpServlet {
  private Producer<String, String> producer;

  @Override
  public void init() throws ServletException {
    super.init();
    this.producer = KafkaProducerFactory.getInstance().getKafkaProducer();
  }

  /**
   * Fully validate the URL and JSON payload
   * If valid, format the incoming **Swipe **data and send it as a payload to a remote queue,
   * and then return success to the client
   */
  @Override
  protected void doPost(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    this.processRequest(request, response);
  }


  private void processRequest(HttpServletRequest request, HttpServletResponse response) throws IOException {
    response.setContentType("application/json");
    ResponseMsg responseMsg = new ResponseMsg();
    Gson gson = new Gson();

    String urlPath = request.getPathInfo();

    // check we have a URL!
    if (urlPath == null || urlPath.isEmpty()) {
      responseMsg.setMessage("missing path parameter: left or right");
      response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      response.getOutputStream().print(gson.toJson(responseMsg));
      response.getOutputStream().flush();
      return;
    }

    // check if URL is valid! "left" or right""
    Pair urlValidationRes = this.isUrlValid(urlPath);
    if (!urlValidationRes.isUrlPathValid()) {
      responseMsg.setMessage("invalid path parameter: should be " + SwipeDetails.LEFT + " or " + SwipeDetails.RIGHT);
      response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      response.getOutputStream().print(gson.toJson(responseMsg));
      response.getOutputStream().flush();
      return;
    }

    String direction = urlValidationRes.getParam();

    // Check if request body/payload is valid, and set corresponding response status & message
    String reqBodyJsonStr = this.getJsonStrFromReq(request);
    boolean isReqBodyValid = this.validateRequestBody(reqBodyJsonStr, response, responseMsg, gson);

    if (!isReqBodyValid) {
      // Send the response status(Failed) & message back to client
      response.getOutputStream().print(gson.toJson(responseMsg));
      response.getOutputStream().flush();
      return;
    }

    // If request body is valid, send the Swipe data to RabbitMQ queue
    if (this.sendMessageToBroker(direction, reqBodyJsonStr, gson)) { //TODO: Check argument type: JsonObject?? String??
      responseMsg.setMessage("Succeeded in sending message to RabbitMQ!");
      response.setStatus(HttpServletResponse.SC_CREATED);
    } else {
      responseMsg.setMessage("Failed to send message to RabbitMQ");
      response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
    }
    response.getOutputStream().print(gson.toJson(responseMsg));
    response.getOutputStream().flush();
  }



  private Pair isUrlValid(String urlPath) {
    /**
     * Check if url path param: {leftorright} has value "left" or "right"
     */
    // urlPath  = "/1/seasons/2019/day/1/skier/123"
    // urlParts = [, 1, seasons, 2019, day, 1, skier, 123]
    String[] urlParts = urlPath.split("/");
    if (urlParts.length == 2 && (urlParts[1].equals(SwipeDetails.LEFT) || urlParts[1].equals(SwipeDetails.RIGHT)))
      return new Pair(true, urlParts[1]);
    return new Pair(false, null);
  }


  private boolean validateRequestBody(String reqBodyJsonStr,HttpServletResponse response, ResponseMsg responseMsg, Gson gson) {
    SwipeDetails swipeDetails = (SwipeDetails) gson.fromJson(reqBodyJsonStr, SwipeDetails.class);

    if (!swipeDetails.isSwiperValid()) {
      responseMsg.setMessage("User not found: invalid swiper id: "+ swipeDetails.getSwiper());
      response.setStatus(HttpServletResponse.SC_NOT_FOUND);
      return false;
    } else if (!swipeDetails.isSwipeeValid()) {
      responseMsg.setMessage("User not found: invalid swipee id: " + swipeDetails.getSwipee());
      response.setStatus(HttpServletResponse.SC_NOT_FOUND);
      return false;
    } else if (!swipeDetails.isCommentValid()) {
      responseMsg.setMessage("Invalid inputs: comment cannot exceed 256 characters");
      response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      return false;
    }
    return true;
  }

  private String getJsonStrFromReq(HttpServletRequest request) throws IOException {
    StringBuilder sb = new StringBuilder();
    String s;
    while ((s = request.getReader().readLine()) != null) {
      sb.append(s);
    }

    return sb.toString();
  }


  private boolean sendMessageToBroker(String direction, String reqBodyJsonStr, Gson gson) {
    SwipeDetails swipeDetails = gson.fromJson(reqBodyJsonStr, SwipeDetails.class);
    swipeDetails.setDirection(direction);
    String message = gson.toJson(swipeDetails);

    try {
      ProducerRecord<String, String> matchesRecord = new ProducerRecord<>("matchesTopic", message);
      producer.send(matchesRecord);

      ProducerRecord<String, String> statsRecord = new ProducerRecord<>("statsTopic", message);
      producer.send(statsRecord);

      return true;
    } catch (Exception e) {
      Logger.getLogger(SwipeServlet.class.getName()).info("Failed to send message to Kafka");
      return false;
    }
  }

  @Override
  public void destroy() {
    if (producer != null) {
      producer.close();
    }
    super.destroy();
  }
}


