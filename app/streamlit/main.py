import streamlit as st
import requests
import sys

sys.path.append(
    r"C:\Users\Sunil Kumar\OneDrive\Documents\Sunil\Repository\Sarvam_Assignment\Smart-ChatBot-AI"
)

from app.database import DBHelper
from app.schemas.request import UserRequest

# Backend API URL
BACKEND_URL = "http://localhost:8000"


def login_page():
    """User Login and Registration Page"""
    st.title("Login to Your Account")
    email = st.text_input("Email")
    password = st.text_input("Password", type="password")

    db_obj = DBHelper()

    if st.button("Login"):
        response = db_obj.get_user(email)
        if response is not None:
            if password == response.password:
                st.success("Logged In!")
                st.session_state["authenticated"] = True
                st.session_state["page"] = "Chat"
                st.session_state["user_name"] = response.name
                st.experimental_set_query_params(page="Chat")
                st.rerun()
            else:
                st.error("Invalid credentials. Try again.")
        else:
            st.error("User NOT found. Please create an account.")

    st.markdown("Don't have an account? Register below.")

    if "register" not in st.session_state:
        st.session_state["register"] = False

    if st.button("Register"):
        st.session_state["register"] = True

    if st.session_state["register"]:
        name = st.text_input("Full Name", type="default", key="name_input")
        email = st.text_input("Email", key="email_input")
        password = st.text_input("Password", type="password", key="password_input")
        confirm_password = st.text_input(
            "Confirm Password", type="password", key="confirm_password_input"
        )
        if st.button("Create Account"):
            if password == confirm_password:
                user = UserRequest(name=name, email=email, password=password)
                if db_obj.add_user(user=user) is not None:
                    st.success("Registered Successfully! Please log in.")
                    st.session_state["register"] = False
                else:
                    st.error("Registration failed. Try again.")
            else:
                st.error("Passwords do not match!")


def user_profile():
    """User Profile Page"""
    st.title("User Profile")
    name = st.text_input("Name")
    company = st.text_input("Company")
    contact_info = st.text_input("Contact Information")
    if st.button("Update Profile"):
        response = requests.post(
            f"{BACKEND_URL}/update_profile",
            json={"name": name, "company": company, "contact_info": contact_info},
        )
        if response.status_code == 200:
            st.success("Profile Updated!")
        else:
            st.error("Update failed. Try again.")


def chatbot_interface():
    """Chatbot Interface"""
    st.title("Chat with AI-Powered Bot")
    chat_history = st.container()
    user_input = st.text_input("Type your message")

    if st.button("Send"):
        if user_input:
            response = requests.post(
                f"{BACKEND_URL}/chat", json={"user_input": user_input}
            )
            if response.status_code == 200:
                bot_response = response.json().get("response", "No response received.")
                chat_history.markdown(f"**You:** {user_input}\n**Bot:** {bot_response}")
            else:
                st.error("Error communicating with chatbot.")


def main():
    """Main Application Logic"""
    st.sidebar.title("Navigation")
    st.sidebar.markdown("🔹 **Welcome to GreenLife Chatbot!**")
    st.sidebar.markdown(
        "📌 Use this chatbot to streamline order placements and queries."
    )
    st.sidebar.divider()

    if "authenticated" not in st.session_state:
        st.session_state["authenticated"] = False
    if "page" not in st.session_state:
        st.session_state["page"] = "Login"

    if st.session_state["authenticated"]:
        st.sidebar.markdown(f"✅ **Logged in as:** {st.session_state['user_name']}")
        chatbot_interface()
    else:
        login_page()


if __name__ == "__main__":
    main()
